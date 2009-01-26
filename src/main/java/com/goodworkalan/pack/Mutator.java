package com.goodworkalan.pack;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * An isolated view of an atomic alteration the contents of a {@link Pack}. In
 * order to allocate, read, write or free blocks, one must create a
 * <code>Mutator</code> by calling {@link Pack#mutate()}.
 */
public final class Mutator
{
    /** The page manager of the pack to mutate. */
    private final Pager pager;
    
    /** A journal to record the isolated mutations of the associated pack. */
    private final Journal journal;

    /** A table that orders allocation pages by the size of bytes remaining. */
    private final BySizeTable allocPagesBySize;
    
    /** A table that orders write pages by the size of bytes remaining. */
    private final BySizeTable writePagesBySize;

    /**
     * A map of addresses to movable position references to the blocks the
     * addresses reference.
     */
    private final SortedMap<Long, Movable> addresses;

    /** A set of pages that need to be flushed to the disk.  */
    private final DirtyPageSet dirtyPages;
    
    /**
     * The per mutator recorder of move nodes that appends the page moves to a
     * linked list of move nodes.
     */
    private final MoveNodeRecorder moveNodeRecorder;
    
    /**
     * The per mutator recorder of move nodes that adjusts the file positions of
     * referenced pages.
     */
    private final PageRecorder pageRecorder;

    /**
     * An iterator over the per pager linked list of latched page moves.
     * Operations that reference pages will used the mutate methods of this move
     * latch list to guard against page movement.
     */
    private MoveLatchIterator moveLatches;

    /**
     * A list of journal entries that write temporary node references for the
     * temporary block allocations of this mutator.
     */
    private final List<Temporary> temporaries;

    /**
     * The address of the last address page used to allocate an address. We give
     * this to the pager when we request an address page to indicate a
     * preference, in hopes of improving locality of reference.
     * 
     * @see Pager#getAddressPage(long)
     */
    private long lastAddressPage;

    /**
     * Create a new mutator to alter the contents of a specific pagers.
     * 
     * @param pager
     *            The page manager of the pack to mutate.
     * @param moveLatchList
     *            A reference to the per pager linked list of latched page
     *            moves.
     * @param moveNodeRecorder
     *            The per mutator recorder of move nodes that appends the page
     *            moves to a linked list of move nodes.
     * @param pageRecorder
     *            The per mtuator recorder of move nodes that adjusts the file
     *            positions of referenced pages.
     * @param journal
     *            A journal to record the isolated mutations of the associated
     *            pack.
     * @param dirtyPages
     *            The set of dirty pages.
     */
    Mutator(Pager pager, MoveLatchIterator moveLatchList, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder,
        Journal journal, DirtyPageSet dirtyPages)
    {
        BySizeTable allocPagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
        BySizeTable writePagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());



        this.pager = pager;
        this.journal = journal;
        this.allocPagesBySize = allocPagesBySize;
        this.writePagesBySize = writePagesBySize;
        this.dirtyPages = dirtyPages;
        this.addresses = new TreeMap<Long, Movable>();
        this.moveNodeRecorder = moveNodeRecorder;
        this.pageRecorder = pageRecorder;
        this.temporaries = new ArrayList<Temporary>();
    }

    /**
     * Return the move latch iterator over the per pager list of move latches.
     * <p>
     * The move latch iterator is lazily constructed to conserve memory and
     * prevent leaks. Each move latch iterator holds a reference to the nodes of
     * the per pager linked list. It will advance through the list when a
     * guarded method is called. The list of move latches itself advances
     * through the list, so that the head moves forward, and the previous nodes
     * are no longer referenced. They can then be reclaimed by the garbage
     * collector.
     * <p>
     * However, if we keep an iterator around after a rollback or commit, it
     * will hold a node reference, but it will not be tracking any pages. We
     * only want to hold node references when the client programmer is actually
     * using the mutator.
     * 
     * @return The move latch iterator over the per pager list of move latches.
     */
    private MoveLatchIterator getMoveLatches()
    {
        if (moveLatches == null)
        {
            CompositeMoveRecorder moveRecorder = new CompositeMoveRecorder();
            
            moveRecorder.add(pageRecorder);
            moveRecorder.add(moveNodeRecorder);
            moveRecorder.add(new BySizeTableRecorder(allocPagesBySize));
            moveRecorder.add(new BySizeTableRecorder(writePagesBySize));
            moveRecorder.add(new JournalRecorder(journal));
            
            moveLatches = pager.getMoveLatchList().newIterator(moveRecorder);
        }
        
        return moveLatches;
    }

    /**
     * Return the pack that this mutator alters.
     * 
     * @return The pack.
     */
    public Pack getPack()
    {
        return pager;
    }

    /**
     * Allocate a block whose address will be returned in the list of temporary
     * blocks when the pack is reopened.
     * <p>
     * I've implemented this using user space, which seems to imply that I don't
     * need to provide this as part of the core. I'm going to attempt to
     * implement it as a user object.
     * 
     * @param blockSize
     *            Size of the temporary block to allocate.
     * 
     * @return The address of the block.
     */
    public long temporary(int blockSize)
    {
        long address = allocate(blockSize);
        
        final Temporary temporary = pager.getTemporary(address);

        getMoveLatches().mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoves)
            {
                journal.write(temporary);
            }
        });
        
        temporaries.add(temporary);
        
        return address;
    }

    /**
     * Allocate a block in the <code>Pack</code> to accommodate a block of the
     * specified block size. This method will reserve a new block and return the
     * address of the block. The block will not be visible to other mutators
     * until the mutator commits it's changes.
     * 
     * @param blockSize
     *            The size of the block to allocate.
     * @return The address of the block.
     */
    public long allocate(int blockSize)
    {
        AddressPage addressPage = null;
        final long address;
        addressPage = pager.getAddressPage(lastAddressPage);
        try
        {
            address = addressPage.reserve(dirtyPages);
        }
        finally
        {
            pager.returnAddressPage(addressPage);
        }
        
        // Add the header size to the block size.
                
        final int fullSize = blockSize + Pack.BLOCK_HEADER_SIZE;
       
        return getMoveLatches().mutate(new Guarded<Long>()
        {
            public Long run(List<MoveLatch> listOfMoves)
            {
                
                // This is unimplemented: Creating a linked list of blocks when
                // the block size exceeds the size of a page.
                
                int pageSize = pager.getPageSize();
                if (fullSize + Pack.BLOCK_PAGE_HEADER_SIZE > pageSize)
                {
                    // Recurse.
                    throw new UnsupportedOperationException();
                }
                
                // If we already have a wilderness data page that will fit the
                // block, use that page. Otherwise, allocate a new wilderness
                // data page for allocation.


                // We know that our already reserved pages are not moving
                // because our page recorder will wait for them to move.

                // We know that the new iterim page is not moving TODO?
                
                InterimPage interim = null;
                long bestFit = allocPagesBySize.bestFit(fullSize);
                if (bestFit == 0L)
                {
                    interim = pager.newInterimPage(new InterimPage(), dirtyPages);
                    pageRecorder.getAllocBlockPages().add(interim.getRawPage().getPosition());
                }
                else
                {
                    interim = pager.getPage(bestFit, InterimPage.class, new InterimPage());
                }
                
                // Allocate a block from the wilderness data page.
                
                interim.allocate(address, fullSize, dirtyPages);
                
                allocPagesBySize.add(interim);
                
                addresses.put(-address, new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0));
                
                return address;
            }
        });
    }

    /**
     * Dereferences the page referenced by the address, adjusting the file
     * position of the page according the list of user move latches.
     * 
     * @param address
     *            The block address.
     * @param userMoveLatches
     *            A list of the move latches that guarded the most recent user
     *            page moves.
     * @return The user block page.
     */
    private UserPage dereference(long address, List<MoveLatch> userMoveLatches)
    {
        // Get the address page.
        AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());

        // Assert that address is not a free address.
        long position = addresses.dereference(address);
        if (position == 0L || position == Long.MAX_VALUE)
        {
            throw new PackException(Pack.ERROR_FREED_FREE_ADDRESS);
        }
        
        // For each move latch in the list of the most recent moves of user
        // pages, enter the latch and record the move. Since user pages only
        // move forward from address region to user region, we are assured that
        // if the from position matches the dereferenced position, that is
        // our dereferenced position is indeed out of date.
        for (MoveLatch latch: userMoveLatches)
        {
            if (latch.getMove().getFrom() == position)
            {
                latch.enter();
                position = latch.getMove().getTo();
            }
        }
    
        return pager.getPage(position, UserPage.class, new UserPage());
    }

    /**
     * Write the remaining bytes of the given source buffer to the block
     * referenced by the given block address. If there are more bytes remaining
     * in the source buffer than the size of the addressed block, no bytes are
     * transferred and a <code>BufferOverflowException</code> is thrown.
     * 
     * @param address
     *            The block address.
     * @param source
     *            The buffer whose remaining bytes are written to the block
     *            referenced by the address.
     * 
     * @throws BufferOverflowException
     *             If there is insufficient space in the block for the remaining
     *             bytes in the source buffer.
     */
    public void write(final long address, final ByteBuffer source)
    {
        getMoveLatches().mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoveLatches)
            {
                // For now, the first test will write to an allocated block, so
                // the write buffer is already there.
                InterimPage interim = null;
                Movable movable = addresses.get(address);
                if (movable == null)
                {
                    movable = addresses.get(-address);
                }
                if (movable == null)
                {
                    // Interim block pages allocated to store writes are tracked
                    // in a separate by size table and a separate set of interim
                    // pages. During commit interim write blocks need only be
                    // copied to the user pages where they reside, while interim
                    // alloc blocks need to be assigned (as a page) to a user
                    // page with space to accommodate them.

                    BlockPage blocks = dereference(address, listOfMoveLatches);
                    int blockSize = blocks.getBlockSize(address);
                   
                    long bestFit = writePagesBySize.bestFit(blockSize);
                    if (bestFit == 0L)
                    {
                        interim = pager.newInterimPage(new InterimPage(), dirtyPages);
                        pageRecorder.getWriteBlockPages().add(interim.getRawPage().getPosition());
                    }
                    else
                    {
                        interim = pager.getPage(bestFit, InterimPage.class, new InterimPage());
                    }
                    
                    interim.allocate(address, blockSize, dirtyPages);
                    
                    if (blockSize < source.remaining() + Pack.BLOCK_HEADER_SIZE)
                    {
                        ByteBuffer copy = ByteBuffer.allocateDirect(blockSize - Pack.BLOCK_HEADER_SIZE);
                        if (blocks.read(address, copy) == null)
                        {
                            throw new IllegalStateException();
                        }
                        copy.flip();
                        interim.write(address, copy, dirtyPages);
                    }

                    movable = new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0);
                    addresses.put(address, movable);
                }
                else
                {
                    interim = pager.getPage(movable.getPosition(pager), InterimPage.class, new InterimPage());
                }
    
                if (!interim.write(address, source, dirtyPages))
                {
                    throw new IllegalStateException();
                }
            }
        });
    }

    /**
     * Read the block at the given address into the a buffer and return the
     * buffer. This method will allocate a byte buffer of the block size.
     * 
     * @param address
     *            The block address.
     * @return A buffer containing the contents of the block.
     */
    public ByteBuffer read(long address)
    {
        ByteBuffer bytes = tryRead(address, null);
        bytes.flip();
        return bytes;
    }

    /**
     * Read the block referenced by the given address into the given destination
     * buffer. If the block size is greater than the bytes remaining in the
     * destination buffer, size of the addressed block, no bytes are transferred
     * and a <code>BufferOverflowException</code> is thrown.
     * 
     * @param address
     *            The address of the block.
     * @param destination
     *            The destination byte buffer.
     * @throws BufferOverflowException
     *             If the size of the block is greater than the bytes remaining
     *             in the destination buffer.
     */
    public void read(long address, ByteBuffer destination)
    {
        if (destination == null)
        {
            throw new NullPointerException();
        }
        tryRead(address, destination);
    }

    /**
     * Read the block at the given address into the given destination buffer. If
     * the given destination buffer is null, this method will allocate a byte
     * buffer of the block size. If the given destination buffer is not null and
     * the block size is greater than the bytes remaining in the destination
     * buffer, size of the addressed block, no bytes are transferred and a
     * <code>BufferOverflowException</code> is thrown.
     * 
     * @param address
     *            The block address.
     * @param destination
     *            The destination buffer or null to indicate that the method
     *            should allocate a destination buffer of block size.
     * @return The given or created destination buffer.
     * @throws BufferOverflowException
     *             If the size of the block is greater than the bytes remaining
     *             in the destination buffer.
     */
    private ByteBuffer tryRead(final long address, final ByteBuffer destination)
    {
        return getMoveLatches().mutate(new Guarded<ByteBuffer>()
        {
            public ByteBuffer run(List<MoveLatch> userMoveLatches)
            {
                ByteBuffer out = null;
                Movable movable = addresses.get(address);
                if (movable == null)
                {
                    movable = addresses.get(-address);
                }
                if (movable == null)
                {
                    AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());
                    long lastPosition = 0L;
                    for (;;)
                    {
                        long actual = addresses.dereference(address);
                        if (actual == 0L || actual == Long.MAX_VALUE)
                        {
                            throw new PackException(Pack.ERROR_READ_FREE_ADDRESS);
                        }

                        if (actual != lastPosition)
                        {
                            UserPage user = pager.getPage(actual, UserPage.class, new UserPage());
                            out = user.read(address, destination);
                            if (out != null)
                            {
                                break;
                            }
                            lastPosition = actual;
                        }
                        else
                        {
                            throw new IllegalStateException();
                        }
                    }
                }
                else
                {
                    InterimPage interim = pager.getPage(movable.getPosition(pager), InterimPage.class, new InterimPage());
                    out = interim.read(address, destination);
                }

                return out;
            }
        });
    }

    /**
     * Free a block in the pack. Free will free a block. The block may have been
     * committed, or it may have been allocated by this mutator. That is, you
     * can free blocks allocated by a mutator, before they are written by a
     * commit or discarded by a rollback.
     * <p>
     * The client programmer is responsible for synchronizing writes and frees.
     * A client program must not free an address that is being freed or written
     * by another mutator.
     * <p>
     * The free is isolated, so that a read of the address of a committed block
     * will still be valid while the free is uncommitted.
     * 
     * @param address
     *            The address of the block to free.
     */
    public void free(final long address)
    {
        // User is not allowed to free named blocks.
        if (pager.getStaticPages().containsValue(address))
        {
            throw new PackException(Pack.ERROR_FREED_STATIC_ADDRESS);
        }

        // Ensure that no pages move.
        getMoveLatches().mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoveLatches)
            {
                // If true, the block was allocated during this mutation and
                // does not require a journaled free.
                boolean unallocate = false;

                // See if the block was allocated during this mutation.  This is
                // indicated by its presence in the address map with the
                // negative value of the address as a key. If not, check to see
                // if there has been a write to the address during this
                // mutation.
                Movable movable = addresses.get(-address);
                if (movable != null)
                {
                    unallocate = true;
                }
                else
                {
                    movable = addresses.get(address);
                }

                // If there is an interim block for the address, we need to free
                // the interim block.
                if (movable != null)
                {
                    // Find the current block position, adjusted for page moves.
                    long position = movable.getPosition(pager);
                    
                    // Figure out which by size table contains the page.  We
                    // will not reinsert the page if it is not already in the by
                    // size table.
                    BySizeTable bySize = unallocate ? allocPagesBySize : writePagesBySize;
                    boolean reinsert = bySize.remove(position) != 0;
                    
                    // Free the block from the interim page.
                    InterimPage interim = pager.getPage(position, InterimPage.class, new InterimPage());
                    interim.free(address, dirtyPages);
                    
                    // We remove and reinsert because if we free from the end of
                    // the block, the bytes remaining will change.
                    if (reinsert)
                    {
                        bySize.add(interim);
                    }
                }

                // If we are freeing a block allocated by this mutator, we
                // simply need to set the file position referenced by the
                // address to zero. Otherwise, we need to write a journaled free
                // to the journal.
                if (unallocate)
                {
                    AddressPage addresses = pager.getPage(-address, AddressPage.class, new AddressPage());
                    addresses.free(address, dirtyPages);
                }
                else
                {
                    UserPage user = dereference(address, listOfMoveLatches);
                    long position = user.getRawPage().getPosition();
                    journal.write(new Free(address, position));
                    pageRecorder.getUserPageSet().add(position);
                }
            }
        });
    }
    
    /**
     * Internal implementation of rollback, guarded by the per pager compact
     * lock and the list of move latches, performs the rollback as described in
     * the public {@link #rollback()} method.
     */
    private void tryRollback()
    {
        // For each address in the isolated address map, if the address is
        // greater than zero, it is an allocation by this mutator, so we need to
        // set the file position referenced by the address to zero.
        for (long address : addresses.keySet())
        {
            if (address > 0)
            {
                break;
            }
            AddressPage addresses = pager.getPage(-address, AddressPage.class, new AddressPage());
            addresses.free(-address, dirtyPages);
            dirtyPages.flushIfAtCapacity();
        }
        
        
        // Each of the allocations of temporary blocks, blocks that are returned
        // by the opener when the file is reopened, needs to be rolled back. 
        for (Temporary temporary : temporaries)
        {
            temporary.rollback(pager);
        }
        
        // Write any dirty pages.
        dirtyPages.flush();
        
        // Put the interim pages we used back into the set of free interim
        // pages.
        pager.getFreeInterimPages().free(pageRecorder.getAllocBlockPages());
        pager.getFreeInterimPages().free(pageRecorder.getWriteBlockPages());
        pager.getFreeInterimPages().free(pageRecorder.getJournalPageSet());
    }
    
    /**
     * Reset this mutator for reuse.
     *
     * @param commit The state of the commit.
     */
    private void clear(Commit commit)
    {
        journal.reset();
        allocPagesBySize.clear();
        writePagesBySize.clear();
        addresses.clear();
        dirtyPages.clear();
        moveNodeRecorder.clear();
        pageRecorder.clear();
        temporaries.clear();
        lastAddressPage = 0;
        moveLatches = null;
    }

    /**
     * Rollback the changes made to the file by this mutator. Rollback will
     * reset the mutator discarding any changes made to the file.
     * <p>
     * After the rollback is complete, the mutator can be reused.
     */
    public void rollback()
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        pager.getCompactLock().readLock().lock();

        try
        {
            getMoveLatches().mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    tryRollback();
                }
            });
        }
        finally
        {
            pager.getCompactLock().readLock().unlock();
        }
        clear(new Commit(pageRecorder, journal, moveNodeRecorder));
    }

    /**
     * Return the count of pages between the address to user boundary and the
     * user to interim boundary.
     * 
     * @return The count of user pages.
     */
    private int getUserPageCount()
    {
        long userPageSize = pager.getInterimBoundary().getPosition()
                          - pager.getUserBoundary().getPosition();
        return (int) (userPageSize / pager.getPageSize());
    }

    /**
     * Map the pages in the set of pages to a soon to be moved interim that is
     * immediately after the data to interim page boundary. Each page in the set
     * will be mapped to a page immediately after the data to interim boundary,
     * incrementing the boundary as the page is allocated.
     * <p>
     * If the interim page is in the list of free interim pages, remove it. We
     * will not lock it. No other mutator will reference a free page because no
     * other mutator is moving pages and no other mutator will be using it for
     * work space.
     * <p>
     * If the page is not in the list of free interim pages, we do have to lock
     * it.
     * 
     * @param moveLatchList
     *            The per pager list of move latches associated with a move
     *            recorder specific to this commit.
     * @param commit
     *            The state of this commit.
     * @param count
     *            The number of pages by which to expand the move the user to
     *            interim boundary into the interim region.
     * @param userFromInterimPagesToMove
     *            A set of the interim pages currently in user whose contents
     *            needs to be moved to a new interim page.
     */
    private void expandUser(MoveLatchIterator moveLatchList, Commit commit, int count, SortedSet<Long> userFromInterimPagesToMove)
    {
        // This invocation is to flush the move list for the current mutator.
        // You may think that this is pointless, but it's not. It will ensure
        // that the relocatable references are all up to date before we try to
        // move.
        
        // If any of the pages we currently referenced are moving those moves
        // will be complete when this call returns.
        
        moveLatchList.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> userMoveLatches)
            {
            }
        });

        // Gather the interim pages that will become data pages, moving the data
        // to interim boundary.
        
        for (int i = 0; i < count; i++)
        {
            // Use the page that is at the data to interim boundary.
            
            long userFromInterimPage = pager.getInterimBoundary().getPosition();
            
            // If it is not in the set of free pages it needs to be moved,
            // so we add it to the set of in use.
            
            if (!pager.getFreeInterimPages().reserve(userFromInterimPage))
            {
                userFromInterimPagesToMove.add(userFromInterimPage);
            }

            // Synapse: What if this page is already in our set? That's
            // fine because we'll first check to see if the page exists
            // as a positive value, then we'll adjust the negative value.
            
            // However, I'm pretty sure that this is never the case, so
            // I'm going to put an assertion here and think about it.
            
            if (pageRecorder.getUserPageSet().contains(userFromInterimPage))
            {
                throw new IllegalStateException();
            }

            commit.getUserFromInterimPages().add(userFromInterimPage);
            
            // Increment the data to interim boundary.

            pager.getInterimBoundary().increment();
        }
    }

    /**
     * Add the interim moves to the per pager list of move latches. This method
     * obtains a new blank interim page from the pager for each of the interim
     * pages that need to be moved to accommodate user region expansion. A move
     * latch is appended to the given head of linked list of move latches
     * indicating the move from the location that will consumed by the user
     * region to the new blank interim page.
     * 
     * @param moveList
     *            The move list.
     * @param iterimMoveLatches
     *            Head of list of iterim move latches.
     * @param userFromInterimPagesToMove
     *            Set of iterim pages that need to be moved.
     */
    private void addIterimMoveLatches(MoveLatchIterator moveList, MoveLatch iterimMoveLatches, SortedSet<Long> userFromInterimPagesToMove)
    {
        if (userFromInterimPagesToMove.size() != 0)
        {
            for (long from : userFromInterimPagesToMove)
            {
                long to = pager.newBlankInterimPage();
                if (userFromInterimPagesToMove.contains(to))
                {
                    throw new IllegalStateException();
                }
                iterimMoveLatches.getLast().extend(new MoveLatch(new Move(from, to), false));
            }
            pager.getMoveLatchList().add(iterimMoveLatches);
            moveList.skip(iterimMoveLatches);
        }
    }

    /**
     * Create interim block pages that will mirror the user pages that need to
     * be moved in order to create new address pages and add it to the
     * allocation pages by size table and the allocation page set. The commit
     * method will allocate a user page for the mirrored page since it is in the
     * allocation page set. We will look this up in {@link #expandAddress} and
     * add a move latch to our list of move latches for user pages that moves
     * the user page at the location of our new address page to the user page
     * chosen by the commit method.
     *
     * @param commit The state of the current commit.
     */
    private void allocMovingUserPageMirrors(Commit commit)
    {
        // For each user page that need to move to create an address page.
        for (long position: commit.getAddressFromUserPagesToMove())
        {
            // Get the user page to move.
            UserPage user = pager.getPage(position, UserPage.class, new UserPage());

            // Allocate an iterim page to act as a mirror.
            InterimPage interim = pager.newInterimPage(new InterimPage(), dirtyPages);

            // Add the interim page to the map of allocation page sets (by size).
            allocPagesBySize.add(interim.getRawPage().getPosition(), user.getRemaining());

            // Add the interim page to the map of allocation pages.
            pageRecorder.getAllocBlockPages().add(interim.getRawPage().getPosition());

            // Map the user page to the interim mirror page.
            Movable movable = new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0);
            commit.getMovingUserPageMirrors().put(user.getRawPage().getPosition(), movable);
        }
    }

    /**
     * Create address pages, extending the address page region by moving the
     * user pages immediately follow after locking the pager to prevent
     * compaction and close.
     * <p>
     * Remember that this method is already guarded in <code>Pager</code> by
     * synchronizing on the set of free addresses.
     * 
     * @param moveList
     *            A list of move latches associated with a move recorder
     *            specific to this address page creation.
     * @param commit
     *            The state of the commit that will record the new address
     *            pages.
     * @param newAddressPageCount
     *            The number of address pages to allocate.
     */
    private SortedSet<Long> tryNewAddressPage(MoveLatchIterator moveList, final Commit commit, int newAddressPageCount)
    {
        final MoveLatch userMoveLatchHead = new MoveLatch(false);

        // Only one thread is allowed to expand the user region at once.

        // Note that we aren't so grabby with the expand mutex during a user
        // commit, only during a new address page creation.

        synchronized (pager.getUserExpandMutex())
        {
            // We are going to create address pages from user pages, so check to
            // see that there are enough user pages. By enough user pages, I
            // mean that if there is one address page total and one user page
            // total, and we are allocating three new address pages, then we do
            // not have enough user pages, free pages, used pages, doesn't
            // matter, simply too few user pages.
            
            int userPageCount = getUserPageCount();
            if (userPageCount < newAddressPageCount)
            {            
                SortedSet<Long> userFromInterimPagesToMove = new TreeSet<Long>();

                // Create new user pages.

                expandUser(moveList, commit, newAddressPageCount - userPageCount, userFromInterimPagesToMove);

                // Append the linked list of moves to the move list.

                addIterimMoveLatches(moveList, userMoveLatchHead, userFromInterimPagesToMove);
             }
        }
        
        // If we have moves to perform append them to the per page list
        // of move latches. It may be the case that all of the needed
        // user pages were marked as empty, so they do not need to move.
        
        if (userMoveLatchHead.getNext() != null)
        {
            moveList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    moveAndUnlatch(userMoveLatchHead);
                }
            });
        }
        
        // The set of newly created address pages.
        SortedSet<Long> newAddressPages = new TreeSet<Long>();
        
        // Now we know we have enough user pages to accommodate our creation of
        // address pages. That is we have enough user pages, full stop. We have
        // not looked at whether they are free or in use.
        
        // Some of those user block pages may not yet exist. We are going to
        // have to wait until they exist before we do anything with with the
        // block pages.

        for (int i = 0; i < newAddressPageCount; i++)
        {
            // The new address page is the user page at the user page boundary.
            long position = pager.getUserBoundary().getPosition();
            
            // Record the new address page.
            newAddressPages.add(position);
            
            // If new address page was not just created by relocating an interim
            // page, then we do not need to reserve it.
            
            if (!commit.getUserFromInterimPages().contains(position))
            {
                // If the position is not in the free page by size, then we'll
                // attempt to reserve it from the list of free user pages.

                if (pager.getFreePageBySize().reserve(position))
                {
                    // This user page needs to be moved.
                    commit.getAddressFromUserPagesToMove().add(position);
                    
                    // Remember that free user pages is a FreeSet which will
                    // remove from a set of available, or add to a set of
                    // positions that should not be made available. 
                    pager.getFreeUserPages().reserve(position);
                }
                else
                {
                    // Was not in set of pages by size.
                    
                    if (!pager.getFreeUserPages().reserve(position))
                    {
                        // Was not in set of empty, so it is in use.

                        commit.getAddressFromUserPagesToMove().add(position);
                    }
                }
            }

            // Move the boundary for user pages.
            pager.getUserBoundary().increment();
        }

        // To move a data page to make space for an address page, we simply copy
        // over the block pages that need to move, verbatim into an interim
        // block page and create a commit. The commit method will see these
        // interim block pages will as allocations, it will allocate the
        // necessary user pages and move them into a new place in the user
        // region.

        // The way that journals are written, vacuums and copies are written
        // before the operations gathered during mutation are written, so we
        // write out our address page initializations now and they will occur
        // after the blocks are copied.
        
        for (long position : newAddressPages)
        {
            journal.write(new CreateAddressPage(position));
        }
        
        // If the new address page is in the set of free block pages or if it is
        // a block page we've just created the page does not have to be moved.

        if (commit.getAddressFromUserPagesToMove().size() != 0)
        {
            moveList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    // Allocate mirrors for the user pages and place them in
                    // the alloc page by size table and the allocation page set
                    // so the commit method will assign a destination user page.
                    
                    allocMovingUserPageMirrors(commit);
                }
            });
        }

        // Run the commit.

        tryCommit(moveList, commit);
        
        return newAddressPages;
    }

    /**
     * Create address pages, extending the address page region by moving the
     * user pages immediately follow after locking the pager to prevent
     * compaction and close.
     * 
     * @param count
     *            The number of address pages to create.
     */
    SortedSet<Long> newAddressPages(int count)
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        pager.getCompactLock().readLock().lock();
        
        try
        {
            final Commit commit = new Commit(pageRecorder, journal, moveNodeRecorder);
            return tryNewAddressPage(pager.getMoveLatchList().newIterator(commit), commit, count); 
        }
        finally
        {
            pager.getCompactLock().readLock().unlock();
        }
    }

    /**
     * Iterate the linked list of moves and move latches moving the pages and
     * then releasing the latches.
     * <p>
     * Here is why this is thread-safe...
     * <p>
     * First the list of moves is appended to the move list. Adding to the move
     * list is an exclusive operation, if there are any guarded operations, such
     * as commit, the append must wait. Now this operation takes place, and if
     * any mutator employing one of the pages commits, then they will wait until
     * the latch is released.
     * 
     * @param head
     *            The head a linked list of move latches.
     */
    private void moveAndUnlatch(MoveLatch head)
    {
        // Iterate through the linked list moving pages and releasing
        // latches.

        while (head.getNext() != null && !head.getNext().isHead())
        {
            // Goto the next node.

            head = head.getNext();
            
            // Get a relocatable page.

            RelocatablePage page = pager.getPage(head.getMove().getFrom(), RelocatablePage.class, new RelocatablePage());
            
            // Please note that relocate simply moves the entire page with
            // an unforced write. There is minimal benefit to the dirty page
            // map since we are writing out the whole page anyway, and
            // because were only going to do a little writing after this, so
            // the chances of caching the dirty page are slim.
            
            page.relocate(head.getMove().getTo());
            
            // Here's why this is tread-safe...
            
            // Anyone referencing the page is going to be waiting for the
            // move to complete, because of the move list. This goes for all
            // readers and writers who might need to dereference an interim
            // page. The mutators holding the interim pages will wait
            // regardless of whether or not they intend to manipulate the
            // pages. Note that a read-only mutator reading only from the
            // user region will not wait, since it will not have interim
            // pages.

            // Therefore, we can relocate these interim pages confident that
            // no one is currently attempting to dereference them.
            
            pager.relocate(head.getMove().getFrom(), head.getMove().getTo());

            pager.setPage(head.getMove().getFrom(), UserPage.class, new UserPage(), dirtyPages, false);

            // Now we can let anyone who is waiting on this interim page
            // through.
            
            head.unlatch();
        }
    }

    /**
     * Assign each remaining unassigned interim block pages to one of the user
     * pages newly created by moving an interim page.
     * 
     * @param commit
     *            The state of the commit.
     */
    private void asssignAllocations(Commit commit)
    {
        // Create a copy of the set of user from interim pages so you can use
        // it as a queue.
        SortedSet<Long> userFromInterimPages = new TreeSet<Long>(commit.getUserFromInterimPages());
        
        // While there are unassigned interim block pages, assign a user page
        // created from a moved interim page.
        while (commit.getUnassignedInterimBlockPages().size() != 0)
        {
            // Shift an unassigned interim page.
            long unassignedInterimPage = commit.getUnassignedInterimBlockPages().first();
            commit.getUnassignedInterimBlockPages().remove(unassignedInterimPage);
            
            // Shift a page created by moving an interim page.
            long userFromInterimPage = userFromInterimPages.first();
            userFromInterimPages.remove(userFromInterimPage);
            
            // Create a movable reference to the user page.
            Movable movable = new Movable(moveNodeRecorder.getMoveNode(), userFromInterimPage, 0);

            // Add the user page to the set of user pages involved in the
            // commit. 
            pageRecorder.getUserPageSet().add(userFromInterimPage);

            // Add the mapping of the interim page to an empty user page.
            commit.getInterimToEmptyUserPage().put(userFromInterimPage, movable);
        }
    }

    /**
     * Find the user page chosen by the commit method as the destination for the
     * allocation page used to mirror a moving user page and add a move latch to
     * indicate the from the of the user page at the new address page position
     * to the chosen user page.
     * <p>
     * Actual mirroring takes place while writing the journal by calling
     * {@link #mirrorUserPagesForMove}.
     * <p>
     * There is no need to record the move addresses in the user page set, since
     * the only move of the page will be the move recored here.
     * 
     * @param commit
     *            Commit map state and move recorder.
     * @param userMoveLatches
     *            The head of a linked list of move latches.
     */
    private void expandAddress(Commit commit, MoveLatch userMoveLatches)
    {
        // A map of user pages to copy.
        Map<Long, Long> copies = new TreeMap<Long, Long>();

        // We put our mirror page into the set of allocation pages, so the
        // commit method has mapped our allocation page to a user page that will
        // hold it. Find the destination user page and note it in the copy map.

        for (Map.Entry<Long, Movable> entry: commit.getMovingUserPageMirrors().entrySet())
        {
            long addressFromUserPage = entry.getKey();
            long mirroredAsAllocation = entry.getValue().getPosition(pager);
            
            Movable movable = commit.getInterimToSharedUserPage().get(mirroredAsAllocation);
            if (movable == null)
            {
                movable = commit.getInterimToEmptyUserPage().get(mirroredAsAllocation);
            }
            if (movable == null)
            {
                throw new IllegalStateException();
            }
            
            long newOrExistingUser = movable.getPosition(pager);
            copies.put(addressFromUserPage, newOrExistingUser);
        }

        // We always perform a move for a user page. It is simplier. Create a
        // move latch that will protect the move of the user page. (No need to
        // use Movable because of the protection of the latch.)
        
        for (Map.Entry<Long, Long> entry: copies.entrySet())
        {
            long addressFromUserPage = entry.getKey();
            long newOrExistingUser = entry.getValue();
            userMoveLatches.getLast().extend(new MoveLatch(new Move(addressFromUserPage, newOrExistingUser), true));
        }
    }

    /**
     * Mirror user block pages that need to be moved to accommodate address
     * region expansion into the interim block pages allocated for mirroring.
     * 
     * @param commit
     *            The state of the commit.
     * @param userPagesMirroredForMove
     *            A set of user pages to record the user pages mirrored by this
     *            method.
     */
    private void mirrorUserPagesForMove(Commit commit, Set<UserPage> userPagesMirroredForMove)
    {
        // For each moving user page, mirror the page into the interim block
        // page allocated for mirroring.

        for (Map.Entry<Long, Movable> entry: commit.getMovingUserPageMirrors().entrySet())
        {
            long addressFromUserPage = entry.getKey();
            long allocation = entry.getValue().getPosition(pager);
            
            InterimPage mirrored = pager.getPage(allocation, InterimPage.class, new InterimPage());
            
            UserPage user = pager.getPage(addressFromUserPage, UserPage.class, new UserPage());
            user.mirror(false, null, mirrored, dirtyPages);

            userPagesMirroredForMove.add(user);
        }
    }

    /**
     * For each mapping of an interim block page to a destination user block
     * page, write a copy operation for each block in the interim block page.
     * 
     * @param commits
     *            A map of interim block pages to destination user block pages.
     */
    private void journalCommits(Map<Long, Movable> commits)
    {
        for (Map.Entry<Long, Movable> entry: commits.entrySet())
        {
            InterimPage interim = pager.getPage(entry.getKey(), InterimPage.class, new InterimPage());
            for (long address: interim.getAddresses())
            {
                journal.write(new Copy(address, entry.getKey(), entry.getValue().getPosition(pager)));
            }
        }
    }

    /**
     * Unlock mirrored pages by calling the {@link UserPage#unmirrored} method
     * on all of the user pages in the mirrored set and release the hard
     * reference to the user page implicit in the set by clearing the set.
     * 
     * @param mirrored
     *            A set of mirrored user pages.
     */
    private void unlockMirrored(Set<UserPage> mirrored)
    {
        for (UserPage user : mirrored)
        {
            user.unmirror();
        }
        mirrored.clear();
    }

    /**
     * Commit the mutations.
     * 
     * @param moveLatchList
     *            The per pager list of move latches associated with a move
     *            recorder specific to this commit method.
     * @param commit
     *            The state of this commit.
     */
    private void tryCommit(MoveLatchIterator moveLatchList, final Commit commit)
    {
        // Start by adding all of the interim block pages to the set of interim
        // block pages whose blocks have not been assigned to a user block page.

        commit.getUnassignedInterimBlockPages().addAll(pageRecorder.getAllocBlockPages());

        // If there are interm block pages with no user block page assigned
        // (true for all but read-only mutators), then try to find user block
        // pages that are empty or have enough space to accomodate the interim
        // block pages. Otherwise, we'll have to expand the user page region.

        if (commit.getUnassignedInterimBlockPages().size() != 0)
        {
            // Mate the interim block pages with user block pages.

            moveLatchList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    // Consolidate pages by using existing, partially filled
                    // pages to store our new block allocations.
    
                    pager.getFreePageBySize().join(allocPagesBySize, pageRecorder.getUserPageSet(), commit.getInterimToSharedUserPage(), moveNodeRecorder.getMoveNode());
                    commit.getUnassignedInterimBlockPages().removeAll(commit.getInterimToSharedUserPage().keySet());
                    
                    // Use free data pages to store the interim pages whose
                    // blocks would not fit on an existing page.
                    
                    pager.newUserPages(commit.getUnassignedInterimBlockPages(), pageRecorder.getUserPageSet(),
                                       commit.getInterimToEmptyUserPage(), moveNodeRecorder.getMoveNode());
                }
            });
        }
    
        // Create the head of list of move latches for interim page moves that
        // we can append to the per pager list of move latches, if necessary.
        
        final MoveLatch interimMoveLatches = new MoveLatch(false);
    
        // If more pages are needed, then we need to extend the user region of
        // the file.

        if (commit.getUnassignedInterimBlockPages().size() != 0)
        {

            // Grab the expand mutex to prevent anyone else from adjusting the
            // user to interim boundary.

            synchronized (pager.getUserExpandMutex())
            {
                // Lock obtained. We might actually have enough pages now, but
                // that is unlikely. Let's make more without checking.
                
                // The set of interim pages that are currently in use and need
                // to be copied into a new empty interim page.
                SortedSet<Long> userFromInterimPagesToMove = new TreeSet<Long>();
                
                // Expand the user region into the interim region to accomodate
                // the new user pages.
                expandUser(moveLatchList, commit, commit.getUnassignedInterimBlockPages().size(), userFromInterimPagesToMove);
                
                // Add move latches to the per pager move latch list for each
                // interim page currently in use whose contents needs to be
                // moved to accomodate the moved user to interim boundary.
                addIterimMoveLatches(moveLatchList, interimMoveLatches, userFromInterimPagesToMove);

                // Allocate a new interim page for each interim page currently
                // in use whose contents needs to be moved to accommodate the
                // moved user to interim boundary.
                asssignAllocations(commit);
            }
        }

        // Move interim pages currently in use to new interim pages to
        // accommodate any new user pages necessary. Note that we left the expand
        // synchronized block and any mutators using the interim blocks we are
        // about to move are waiting for us to unlatch the latches we added to
        // the per pager list of move latches.
        
        if (interimMoveLatches.getNext() != null)
        {
            moveLatchList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    moveAndUnlatch(interimMoveLatches);
                }
            });
        }

        // Create the head of list of move latches for user page moves that we
        // can append to the per pager list of move latches, if necessary.
        
        final MoveLatch userMoveLatches = new MoveLatch(false);
        
        // If commit is part of the expansion of the address region and there
        // are user pages that need to move to accomdate address pages, add the
        // user moves to the per pager list of move latches.
        
        if (commit.getAddressFromUserPagesToMove().size() != 0)
        {
            moveLatchList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    // Add all of the user page moves to the list of move
                    // latches for user pages.

                    expandAddress(commit, userMoveLatches);
                }
            });

            // Append the user page moves to the per pager list of move latches.
            pager.getMoveLatchList().add(userMoveLatches);

            // Skip the user move latches we just added.
            moveLatchList.skip(userMoveLatches);
        }

        // Writing of mirrors, journal completion, move recording, journal
        // playback, resources returned to the pager.

        moveLatchList.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> userMoveLatches)
            {
                // Write a terminate to end the playback loop. This
                // terminate is the true end of the journal.
                journal.write(new Terminate());
    
                // Grab the current position of the journal. This is the
                // actual start of playback.
                long beforeVacuum = journal.getJournalPosition();
                
                // The set of pages mirrored for vacuum.
                Set<UserPage> pagesMirroredForVacuum = new HashSet<UserPage>();
                
                    
                // For each of the interim pages mapped to a user page that
                // already contains user blocks from other mutators, attempt to
                // mirror the page for vaccum.
                for (Map.Entry<Long, Movable> entry: commit.getInterimToSharedUserPage().entrySet())
                {
                    // Mirror the shared user page.
                    UserPage user = pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage());
                    Mirror mirror = user.mirror(true, pager, null, dirtyPages);

                    // If mirror returns null, the user page did not need a
                    // vacuum, there were no free blocks surrounded by employed
                    // bocks. If mirror returns non null, then we note that the
                    // joruanl needs to add the vacuum to the list of vacuums.
                    if (mirror != null)
                    {
                        journal.write(new AddVacuum(mirror, user));
                        pagesMirroredForVacuum.add(user);
                    }
                }
                
                // Get the current position after all the vacuum operations.
                // Once vacuum is performed, we force the file and update the
                // journal so that playback begins at the vacuum operation,
                // which does nothing, since we skipped the list of add vacuums.
                // The vacuum operation will copy the mirrored pages in the
                // interim region back into their user block pages, but
                // appending them, eliminating the free block gaps.
                long afterVacuum = journal.getJournalPosition(); 
                journal.write(new Vacuum(afterVacuum));
                
                // Currently a three stage commit, stopping after vacuum, as
                // well as after the initial journal write.
                journal.write(new Terminate());
                
                // The list of user pages that were mirrored for relocation.
                Set<UserPage> userPagesMirroredForMove = new HashSet<UserPage>();
               
                // Write the mirror of user pages for relocation to the journal.
                if (commit.getAddressFromUserPagesToMove().size() != 0)
                {
                    mirrorUserPagesForMove(commit, userPagesMirroredForMove);
                }

                // Write out a copy operation for each of the interim blocks in
                // both of the maps of interim block pages to destination user
                // block pages.
                journalCommits(commit.getInterimToSharedUserPage());
                journalCommits(commit.getInterimToEmptyUserPage());
               
                // Interim block pages allocated to store writes are written
                // into place using address lookup to find the destination user
                // block page. No vacuum of destination pages.
                
                // (TODO Why not vacuum? Can't you check and see if necessary?)
                for (long position: pageRecorder.getWriteBlockPages())
                {
                    InterimPage interim = pager.getPage(position, InterimPage.class, new InterimPage());
                    for (long address: interim.getAddresses())
                    {
                        journal.write(new Write(address, position));
                    }
                }
    
                // Record all of the user moves now. The moves are not used to
                // determine the location of the vacuums and block commits
                // above. Those are written with adjusted page positions. We
                // write the move list here, then jump to the start of the
                // journal, where the recored free and temporary operations have
                // been written.
                MoveNode moveNode = moveNodeRecorder.getFirstMoveNode();
                while (moveNode.getNext() != null)
                {
                    moveNode = moveNode.getNext();
                    journal.write(new AddMove(moveNode.getMove()));
                }
    
                // Need to use the entire list of moves since the start
                // of the journal to determine the actual journal start.
                long journalStart = journal.getJournalStart().getPosition(pager);
                journal.write(new NextOperation(journalStart));
    
                // Create a next pointer to point at the start of operations.
                Pointer header = pager.getJournalHeaders().allocate();
                header.getByteBuffer().putLong(beforeVacuum);
                dirtyPages.flush(header);
                
                // Create a journal player.
                Player player = new Player(pager, header, dirtyPages);
                
                // First do the vacuums. We'll hit a terimate.
                player.vacuum();
                
                // We can unlock any user block pages mirrored for vacuum.
                unlockMirrored(pagesMirroredForVacuum);

                // Then do everything else.
                player.commit();

                // We can unlock any user block pages mirroed for address move.
                unlockMirrored(userPagesMirroredForMove);

                // Unlock any addresses that were returned as free to their
                // address pages, but were locked to prevent the commit of a
                // reallocation until this commit completed.

                pager.getAddressLocker().unlock(player.getAddressSet());
                
                // TODO Are there special user pages to return for address
                // expansion?
                
                // TODO Make this a method.
                // Return all the interim pages. TODO Who resets them to zero?
                pager.getFreeInterimPages().free(commit.getInterimToSharedUserPage().keySet());
                for (Map.Entry<Long, Movable> entry: commit.getInterimToSharedUserPage().entrySet())
                {
                    pager.returnUserPage(pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage()));
                }

                pager.getFreeInterimPages().free(commit.getInterimToEmptyUserPage().keySet());
                for (Map.Entry<Long, Movable> entry: commit.getInterimToEmptyUserPage().entrySet())
                {
                    pager.returnUserPage(pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage()));
                }
                
                // TODO Make this a method?
                pager.getFreeInterimPages().free(pageRecorder.getJournalPageSet());
                pager.getFreeInterimPages().free(pageRecorder.getWriteBlockPages());
            }
        });

        // Unlatch any user move latches obtained during this commit. The list
        // will only contain user move latches if this is an address region
        // expansion and user pages were moved to accomodate the address region
        // expansion.
        
        MoveLatch userMoveLatch = userMoveLatches;
        while (userMoveLatch.getNext() != null && !userMoveLatch.getNext().isHead())
        {
            userMoveLatch.getNext().unlatch();
            userMoveLatch = userMoveLatch.getNext();
        }
    }

    /**
     * Commit the changes made to the file by this mutator. Commit will make the
     * changes made by this mutator visible to all other mutators.
     * <p>
     * After the commit is complete, the mutator may be reused.
     */
    public void commit()
    {
        // Create a commit structure to track the state of the commit.
        final Commit commit = new Commit(pageRecorder, journal, moveNodeRecorder);

        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        pager.getCompactLock().readLock().lock();

        try
        {
            // Create a move latch list from our move latch list, with the
            // commit structure as the move recoder, so that page moves by other
            // committing mutators will be reflected in state of the commit.
            tryCommit(pager.getMoveLatchList().newIterator(commit), commit);
        }
        finally
        {
            pager.getCompactLock().readLock().unlock();
        }

        clear(commit);
    }
}

/* vim: set tw=80 : */
