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
    final Pager pager;
    
    /** A journal to record the isolated mutations of the associated pack. */
    final Journal journal;

    final BySizeTable allocPagesBySize;
    
    final BySizeTable writePagesBySize;

    /**
     * A map of addresses to movable position references to the blocks the
     * addresses reference.
     */
    final SortedMap<Long, Movable> mapOfAddresses;

    /** A set of pages that need to be flushed to the disk.  */
    final DirtyPageSet dirtyPages;
    
    final MoveNodeRecorder moveNodeRecorder;
    
    final PageRecorder pageRecorder;
    
    final MoveLatchList listOfMoves;
    
    final List<Temporary> listOfTemporaries;
    
    long lastPointerPage;
    
    public Mutator(Pager pager, MoveLatchList listOfMoves, MoveNodeRecorder moveNodeRecorder, PageRecorder pageRecorder, Journal journal, DirtyPageSet dirtyPages)
    {
        BySizeTable allocPagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
        BySizeTable writePagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());

        CompositeMoveRecorder moveRecorder = new CompositeMoveRecorder();
        moveRecorder.add(pageRecorder);
        moveRecorder.add(moveNodeRecorder);
        moveRecorder.add(new BySizeTableRecorder(allocPagesBySize));
        moveRecorder.add(new BySizeTableRecorder(writePagesBySize));
        moveRecorder.add(new JournalRecorder(journal));
        this.pager = pager;
        this.journal = journal;
        this.allocPagesBySize = allocPagesBySize;
        this.writePagesBySize = writePagesBySize;
        this.dirtyPages = dirtyPages;
        this.mapOfAddresses = new TreeMap<Long, Movable>();
        this.moveNodeRecorder = moveNodeRecorder;
        this.listOfMoves = new MoveLatchList(moveRecorder, listOfMoves);
        this.pageRecorder = pageRecorder;
        this.listOfTemporaries = new ArrayList<Temporary>();
    }
    
    public Schema getSchema()
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

        listOfMoves.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoves)
            {
                journal.write(temporary);
            }
        });
        
        listOfTemporaries.add(temporary);
        
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
        addressPage = pager.getAddressPage(lastPointerPage);
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
       
        return listOfMoves.mutate(new Guarded<Long>()
        {
            public Long run(List<MoveLatch> listOfMoves)
            {
                
                // This is unimplemented: Creating a linked list of blocks
                // when the block size exceeds the size of a page.
                
                int pageSize = pager.getPageSize();
                if (fullSize + Pack.BLOCK_PAGE_HEADER_SIZE > pageSize)
                {
                    // Recurse.
                    throw new UnsupportedOperationException();
                }
                
                // If we already have a wilderness data page that will fit
                // the block, use that page. Otherwise, allocate a new
                // wilderness data page for allocation.
                
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
                
                mapOfAddresses.put(-address, new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0));
                
                return address;
            }
        });
    }

    private UserPage dereference(long address, List<MoveLatch> listOfMoveLatches)
    {
        AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());

        long position = addresses.dereference(address);
        if (position == 0L || position == Long.MAX_VALUE)
        {
            throw new PackException(Pack.ERROR_FREED_FREE_ADDRESS);
        }
        
        for (MoveLatch latch: listOfMoveLatches)
        {
            if (latch.getMove().getFrom() == position)
            {
                latch.enter();
                position = latch.getMove().getTo();
            }
        }
    
        return pager.getPage(position, UserPage.class, new UserPage());
    }

    // TODO Write at offset.
    public void write(final long address, final ByteBuffer src)
    {
        listOfMoves.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoveLatches)
            {
                // For now, the first test will write to an allocated block, so
                // the write buffer is already there.
                InterimPage interim = null;
                Movable movable = mapOfAddresses.get(address);
                if (movable == null)
                {
                    movable = mapOfAddresses.get(-address);
                }
                if (movable == null)
                {
                    // Interim block pages allocated to store writes are tracked
                    // in a separate by size table and a separate set of
                    // interim pages. During commit interim write blocks need
                    // only be copied to the user pages where they reside, while
                    // interim alloc blocks need to be assigned (as a page) to a
                    // user page with space to accommodate them.

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
                    
                    if (blockSize < src.remaining() + Pack.BLOCK_HEADER_SIZE)
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
                    mapOfAddresses.put(address, movable);
                }
                else
                {
                    interim = pager.getPage(movable.getPosition(pager), InterimPage.class, new InterimPage());
                }
    
                if (!interim.write(address, src, dirtyPages))
                {
                    throw new IllegalStateException();
                }
            }
        });
    }
    
    public ByteBuffer read(long address)
    {
        ByteBuffer bytes = tryRead(address, null);
        bytes.flip();
        return bytes;
    }
    
    public void read(long address, ByteBuffer bytes)
    {
        tryRead(address, bytes);
    }

    private ByteBuffer tryRead(final long address, final ByteBuffer bytes)
    {
        return listOfMoves.mutate(new Guarded<ByteBuffer>()
        {
            public ByteBuffer run(List<MoveLatch> listOfMoveLatches)
            {
                ByteBuffer out = null;
                Movable movable = mapOfAddresses.get(address);
                if (movable == null)
                {
                    movable = mapOfAddresses.get(-address);
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
                            out = user.read(address, bytes);
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
                    out = interim.read(address, bytes);
                }

                return out;
            }
        });
    }

    public void free(final long address)
    {
        if (pager.isStaticPageAddress(address))
        {
            throw new PackException(Pack.ERROR_FREED_STATIC_ADDRESS);
        }
        listOfMoves.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoveLatches)
            {
                boolean unallocated = false;

                Movable movable = mapOfAddresses.get(-address);
                if (movable != null)
                {
                    unallocated = true;
                }
                else
                {
                    movable = mapOfAddresses.get(address);
                }

                if (movable != null)
                {
                    long position = movable.getPosition(pager);
                    
                    BySizeTable bySize = unallocated ? allocPagesBySize : writePagesBySize;
                    boolean reinsert = bySize.remove(position) != 0;
                    
                    InterimPage interim = pager.getPage(position, InterimPage.class, new InterimPage());
                    interim.free(address, dirtyPages);
                    
                    if (reinsert)
                    {
                        bySize.add(interim);
                    }
                }
                 
                if (unallocated)
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
    
    private void tryRollback()
    {
        for (long address : mapOfAddresses.keySet())
        {
            if (address > 0)
            {
                break;
            }
            AddressPage addresses = pager.getPage(-address, AddressPage.class, new AddressPage());
            addresses.free(-address, dirtyPages);
            dirtyPages.flushIfAtCapacity();
        }
        
        for (Temporary temporary : listOfTemporaries)
        {
            temporary.rollback(pager);
        }
        
        dirtyPages.flush();
        
        pager.getFreeInterimPages().free(pageRecorder.getAllocBlockPages());
        pager.getFreeInterimPages().free(pageRecorder.getWriteBlockPages());
        pager.getFreeInterimPages().free(pageRecorder.getJournalPageSet());
    }
    
    private void clear(Commit commit)
    {
        journal.reset();
        allocPagesBySize.clear();
        writePagesBySize.clear();
        mapOfAddresses.clear();
        dirtyPages.clear();
        moveNodeRecorder.clear();
        pageRecorder.clear();
        listOfTemporaries.clear();
        lastPointerPage = 0;
    }

    public void rollback()
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        pager.getCompactLock().readLock().lock();

        try
        {
            listOfMoves.mutate(new GuardedVoid()
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
     * Return the count of pages between the address to user boundary
     * and the user to interim boundary.
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
    private void expandUser(MoveLatchList moveLatchList, Commit commit, int count, SortedSet<Long> userFromInterimPagesToMove)
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
     * Add the interim moves to the per pager list of move latches.
     * 
     * @param moveList
     *            The move list.
     * @param iterimMoveLatches
     *            Head of list of iterim move latches.
     * @param userFromInterimPagesToMove
     *            Set of iterim pages that need to be moved.
     */
    private void addIterimMoveLatches(MoveLatchList moveList, MoveLatch iterimMoveLatches, SortedSet<Long> userFromInterimPagesToMove)
    {
        if (userFromInterimPagesToMove.size() != 0)
        {
            buildInterimMoveLatchList(userFromInterimPagesToMove, iterimMoveLatches);
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
    private SortedSet<Long> tryNewAddressPage(MoveLatchList moveList, final Commit commit, int newAddressPageCount)
    {
        final MoveLatch userMoveLatchHead = new MoveLatch(false);

        // Only one thread is allowed to expand the user region at once.

        // Note that we aren't so grabby with the expand mutex during a user
        // commit, only during a new address page creation.

        synchronized (pager.getExpandMutex())
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
            commit.getAddressSet().add(position);
            
            // If new address page was not just created by relocating an interim
            // page, then we do not need to reserve it.
            
            if (!commit.getUserFromInterimPages().contains(position))
            {
                // If the position is not in the free page by size, then we'll
                // attempt to reserve it from the list of free user pages.

                if (pager.getFreePageBySize().reserve(position))
                {
                    // Remember that free user pages is a FreeSet which will
                    // remove from a set of available, or add to a set of
                    // positions that should not be made available. 
                    
                    pager.getFreeUserPages().reserve(position);
                    
                    // TODO Doesn't this mean that it is in use?
                }
                else
                {
                    // Was not in set of pages by size.
                    
                    if (!pager.getFreeUserPages().reserve(position))
                    {
                        // Was not in set of empty, so it is in use.

                        // TODO Rename getInUseAddressSet.
                        
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
        // necessary user pages and move them into a new place in the user region.

        // The way that journals are written, vacuums and copies are written
        // before the operations gathered during mutation are written, so we
        // write out our address page initializations now and they will occur
        // after the blocks are copied.
        
        for (long position : commit.getAddressSet())
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
        
        return commit.getAddressSet();
    }

    /**
     * Create address pages, extending the address page region by moving
     * the user pages immediately follow after locking the pager to
     * prevent compaction and close.  
     *
     * @param count The number of address pages to create.
     */
    SortedSet<Long> newAddressPages(int count)
    {
        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        pager.getCompactLock().readLock().lock();
        
        try
        {
            final Commit commit = new Commit(pageRecorder, journal, moveNodeRecorder);
            return tryNewAddressPage(new MoveLatchList(commit, listOfMoves), commit, count); 
        }
        finally
        {
            pager.getCompactLock().readLock().unlock();
        }
    }
    
    private Map<Long, Long> associate(Commit commit)
    {
        SortedSet<Long> setOfGathered = new TreeSet<Long>(commit.getUserFromInterimPages());
        Map<Long, Long> mapOfCopies = new TreeMap<Long, Long>();
        while (commit.getUnassignedInterimBlockPages().size() != 0)
        {
            long interimAllocation = commit.getUnassignedInterimBlockPages().first();
            commit.getUnassignedInterimBlockPages().remove(interimAllocation);
            
            long soonToBeCreatedUser = setOfGathered.first();
            setOfGathered.remove(soonToBeCreatedUser);
            
            mapOfCopies.put(interimAllocation, soonToBeCreatedUser);
        }
        return mapOfCopies;
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
    
    private void buildInterimMoveLatchList(SortedSet<Long> userFromInterimPagesToMove, MoveLatch iterimMoveLatches)
    {
        // For the set of pages in use, add the page to the move list.
        for (long from : userFromInterimPagesToMove)
        {
            long to = pager.newBlankInterimPage();
            if (userFromInterimPagesToMove.contains(to))
            {
                throw new IllegalStateException();
            }
            iterimMoveLatches.getLast().extend(new MoveLatch(new Move(from, to), false));
        }
    }

    private void asssignAllocations(Commit commit)
    {
        Map<Long, Long> mapOfCopies = associate(commit);
        
        for (Map.Entry<Long, Long> copy: mapOfCopies.entrySet())
        {
            long iterimAllocation = copy.getKey();
            long soonToBeCreatedUser = copy.getValue();

            Movable movable = new Movable(moveNodeRecorder.getMoveNode(), soonToBeCreatedUser, 0);

            // Add the page to the set of pages used to track the pages
            // referenced in regards to the move list. We are going to move
            // this page and we are aware of this move. Negating the value
            // tells us not adjust our own move list for the first move
            // detected for this position.

            // TODO Only add as negative if we are going to observe the move.
            pageRecorder.getUserPageSet().add(soonToBeCreatedUser);

            commit.getEmptyMap().put(iterimAllocation, movable);
        }
    }
    
    /**
     * Find the user page choosen by the commit method as the destination for
     * the allocation page used to mirror a moving user page and add a move
     * latch to indicate the from the of the user page at the new address page
     * position to the choosen user page. 
     * <p>
     * Actual mirroring takes place while writing the journal by calling {@link
     * #mirrorUserPagesForMove}.
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
            
            Movable movable = commit.getVacuumMap().get(mirroredAsAllocation);
            if (movable == null)
            {
                movable = commit.getEmptyMap().get(mirroredAsAllocation);
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
     * Mirror user block pages that need to be moved to accomodate address region
     * expansion into the interim block pages allocated for mirroring.
     *
     * @param commit The state of the commit.
     * @param userPagesMirroredForMove A set of user pages to record the user pages mirrored
     * by this method.
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

    private void journalCommits(Map<Long, Movable> mapOfCommits)
    {
        for (Map.Entry<Long, Movable> entry: mapOfCommits.entrySet())
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
     * refernece to the user page implicit in the set by clearing the set.
     *
     * @param mirrored A set of mirrored user pages.
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
     *            The per pager list of move latches assocated with a move
     *            recorder specific to this commit method.
     * @param commit
     *            The state of this commit.
     */
    private void tryCommit(MoveLatchList moveLatchList, final Commit commit)
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
    
                    pager.getFreePageBySize().join(allocPagesBySize, pageRecorder.getUserPageSet(), commit.getVacuumMap(), moveNodeRecorder.getMoveNode());
                    commit.getUnassignedInterimBlockPages().removeAll(commit.getVacuumMap().keySet());
                    
                    // Use free data pages to store the interim pages whose
                    // blocks would not fit on an existing page.
                    
                    pager.newUserPages(commit.getUnassignedInterimBlockPages(), pageRecorder.getUserPageSet(),
                                       commit.getEmptyMap(), moveNodeRecorder.getMoveNode());
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

            synchronized (pager.getExpandMutex())
            {
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
                // in use whose contents needs to be moved to accomodate the
                // moved user to interim boundary.
                asssignAllocations(commit);
            }
        }

        // Move interim pages currently in use to new interim pages to
        // accomodate any new user pages necessary. Note that we left the expand
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
                
                // Create a vacuum operation for all the vacuums.
                Set<UserPage> setOfMirroredVacuumPages = new HashSet<UserPage>();
                
                // TODO Do I make sure that mirroring in included before 
                // vacuum in recovery as well?
                // TODO No. Just make addresses go first. Negative journal.

                for (Map.Entry<Long, Movable> entry: commit.getVacuumMap().entrySet())
                {
                    UserPage user = pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage());
                    Mirror mirror = user.mirror(true, pager, null, dirtyPages);
                    if (mirror != null)
                    {
                        journal.write(new AddVacuum(mirror, user));
                        setOfMirroredVacuumPages.add(user);
                    }
                }
                
                long afterVacuum = journal.getJournalPosition(); 
                
                // Write out all your allocations from above. Each of them
                // becomes an action. Read the interim page and copy the data
                // over to the data page.
                
                // Here we insert the vacuum break. During a recovery, the data
                // pages will be recreated without a reference to their vacuumed
                // page.
    
                // Although, I suppose the vacuum page reference could simply be
                // a reference to the data page.
    
                // Two ways to deal with writing to a vacuumed page. One it to
                // overwrite the vacuum journal. The other is to wait until the
                // vacuumed journal is written.
    
                journal.write(new Vacuum(afterVacuum));
                
                journal.write(new Terminate());
                
                // The list of user pages that were mirrored for relcation.
                Set<UserPage> userPagesMirroredForMove = new HashSet<UserPage>();
               
                // Write the mirror of user pages for relocation to the journal.
                if (commit.getAddressFromUserPagesToMove().size() != 0)
                {
                    mirrorUserPagesForMove(commit, userPagesMirroredForMove);
                }

                journalCommits(commit.getVacuumMap());
                journalCommits(commit.getEmptyMap());
               
                // Interim block pages allocated to store writes need only be
                // written into place using address lookup to find the user
                // block page.

                for (long position: pageRecorder.getWriteBlockPages())
                {
                    InterimPage interim = pager.getPage(position, InterimPage.class, new InterimPage());
                    for (long address: interim.getAddresses())
                    {
                        journal.write(new Write(address, position));
                    }
                }
    
                // Create the list of moves.
                MoveNode iterator = moveNodeRecorder.getFirstMoveNode();
                while (iterator.getNext() != null)
                {
                    iterator = iterator.getNext();
                    journal.write(new AddMove(iterator.getMove()));
                }
    
                // Need to use the entire list of moves since the start
                // of the journal to determine the actual journal start.
                
                long journalStart = journal.getJournalStart().getPosition(pager);
                journal.write(new NextOperation(journalStart));
    
                // Create a next pointer to point at the start of operations.
                Pointer header = pager.getJournalHeaders().allocate();
                header.getByteBuffer().putLong(beforeVacuum);
                dirtyPages.flush(header);
                
                Player player = new Player(pager, header, dirtyPages);
                
                // Obtain a journal header and record the head.
                
                // First do the vacuums.
                player.vacuum();
                
                unlockMirrored(setOfMirroredVacuumPages);

                // Then do everything else.
                player.commit();

                unlockMirrored(userPagesMirroredForMove);

                // Unlock any addresses that were returned as free to their
                // address pages, but were locked to prevent the commit of a
                // reallocation until this commit completed.

                pager.getAddressLocker().unlock(player.getAddressSet());
                
                if (!commit.isAddressExpansion())
                {
                    // TODO Which pages to I return here?
                }
                else
                {
                    pager.getFreeInterimPages().free(commit.getVacuumMap().keySet());
                    pager.getFreeInterimPages().free(commit.getEmptyMap().keySet());
                    for (Map.Entry<Long, Movable> entry: commit.getVacuumMap().entrySet())
                    {
                        pager.returnUserPage(pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage()));
                    }
                    for (Map.Entry<Long, Movable> entry: commit.getEmptyMap().entrySet())
                    {
                        pager.returnUserPage(pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage()));
                    }
                }
                
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

    public void commit()
    {
        final Commit commit = new Commit(pageRecorder, journal, moveNodeRecorder);

        // Obtain shared lock on the compact lock, preventing pack file
        // vacuum for the duration of the address page allocation.

        pager.getCompactLock().readLock().lock();

        try
        {
            tryCommit(new MoveLatchList(commit, listOfMoves), commit);
        }
        finally
        {
            pager.getCompactLock().readLock().unlock();
        }

        clear(commit);
    }
}

/* vim: set tw=80 : */
