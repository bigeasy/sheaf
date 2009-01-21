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
                    pageRecorder.getAllocationPageSet().add(interim.getRawPage().getPosition());
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

    // FIXME Write at offset.
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
                    BlockPage blocks = dereference(address, listOfMoveLatches);
                    int blockSize = blocks.getBlockSize(address);
                   
                    long bestFit = writePagesBySize.bestFit(blockSize);
                    if (bestFit == 0L)
                    {
                        interim = pager.newInterimPage(new InterimPage(), dirtyPages);
                        pageRecorder.getWritePageSet().add(interim.getRawPage().getPosition());
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

    public ByteBuffer tryRead(final long address, final ByteBuffer bytes)
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
        
        pager.getFreeInterimPages().free(pageRecorder.getAllocationPageSet());
        pager.getFreeInterimPages().free(pageRecorder.getWritePageSet());
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
    
    private int getUserPageCount()
    {
        long userPageSize = pager.getInterimBoundary().getPosition()
                          - pager.getUserBoundary().getPosition();
        return (int) (userPageSize / pager.getPageSize());
    }
    
    private void expandUser(MoveLatchList listOfMoves, Commit commit, int count, SortedSet<Long> setOfInUse)
    {
        // This invocation is to flush the move list for the current
        // mutator. You may think that this is pointless, but it's
        // not. It will ensure that the relocatable references are
        // all up to date before we try to move.
        
        // If any of the pages we currently referenced are moving
        // those moves will be complete when this call returns.
        
        listOfMoves.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> userMoveLatches)
            {
            }
        });

        // Gather the interim pages that will become data pages, moving the
        // data to interim boundary.
        
        gatherPages(count, setOfInUse, commit.getUserFromInterimPages());
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
            pager.getMoveList().add(iterimMoveLatches);
            moveList.skip(iterimMoveLatches);
        }
    }

    private void allocateMirrors(Commit commit)
    {
        for (long position: commit.getInUseAddressSet())
        {
            UserPage user = pager.getPage(position, UserPage.class, new UserPage());
            InterimPage interim = pager.newInterimPage(new InterimPage(), dirtyPages);
            allocPagesBySize.add(interim.getRawPage().getPosition(), user.getRemaining());
            pageRecorder.getAllocationPageSet().add(interim.getRawPage().getPosition());
            commit.getAddressMirrorMap().put(user.getRawPage().getPosition(), new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0));
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

        // Note that we aren't so grabby with the expand lock during a
        // user commit, only during a new address page creation.

        pager.getExpandLock().lock();

        try
        {
            // We are going to create address pages from user pages, so
            // check to see that there are enough user pages, free or
            // employed.
            
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
        finally
        {
            pager.getExpandLock().unlock();
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
        
        // Now we know we have enough user pages to accommodate our
        // creation of address pages. That is we have enough user pages,
        // full stop. We not looked at whether they are free or in use.
        
        // Some of those user block pages may not yet exist. We are
        // going to have to wait until they exist before we do anything
        // with with the block pages.

        for (int i = 0; i < newAddressPageCount; i++)
        {
            // The new address page is the user page at the user page boundary.
            long position = pager.getUserBoundary().getPosition();
            
            // Record the new address page.
            commit.getAddressSet().add(position);
            
            // If new address page was not just created by relocating an
            // interim page, then we do not need to reserve it.
            
            if (!commit.getUserFromInterimPages().contains(position))
            {
                // If the position is not in the free page by size, then
                // we'll attempt to reserve it from the list of free
                // user pages.

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
                        
                        commit.getInUseAddressSet().add(position);
                    }
                }
            }

            // Move the boundary for user pages.

            pager.getUserBoundary().increment();
        }

        // To move a data page to make space for an address page, we
        // simply copy over the block pages that need to move, verbatim
        // into an interim block page and create a commit. The block
        // pages will operate as allocations, moving into some area
        // within the user region. The way that journals are written,
        // vacuums and commits take place before the operations written,
        // so we write out our address page initializations now.
        
        for (long position : commit.getAddressSet())
        {
            journal.write(new CreateAddressPage(position));
        }
        
        // If the new address page is in the set of free block pages or
        // if it is a block page we've just created the page does not
        // have to be moved.

        moveList.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoveLatches)
            {
                allocateMirrors(commit);
            }
        });

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
    public SortedSet<Long> newAddressPages(int count)
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
     * @param setOfPages
     *            The set of pages that needs to be moved or copied into a page
     *            in data region of the file.
     * @param setOfMovingPages
     *            A set that will keep track of which pages this mutation
     *            references used in conjunction with the move list.
     * @param mapOfPages
     *            A map that associates one of the pages with an interim page
     *            that will be converted to a data page.
     * @param userFromInterimPagesToMove
     *            A set of the interim pages that need to be moved to a new
     *            interim pages as opposed to pages that were free.
     * @param userFromInterimPages
     *            A set of the newly created data positions.
     */
    private void gatherPages(int count, Set<Long> userFromInterimPagesToMove, SortedSet<Long> userFromInterimPages)
    {
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

            userFromInterimPages.add(userFromInterimPage);
            
            // Increment the data to interim boundary.

            pager.getInterimBoundary().increment();
        }
    }
    
    private Map<Long, Long> associate(Commit commit)
    {
        SortedSet<Long> setOfGathered = new TreeSet<Long>(commit.getUserFromInterimPages());
        Map<Long, Long> mapOfCopies = new TreeMap<Long, Long>();
        while (commit.getUnassignedSet().size() != 0)
        {
            long interimAllocation = commit.getUnassignedSet().first();
            commit.getUnassignedSet().remove(interimAllocation);
            
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

            Movable movable = new Movable(moveNodeRecorder.getMoveNode(),
                                          soonToBeCreatedUser,
                                          0);
            // Add the page to the set of pages used to track the pages
            // referenced in regards to the move list. We are going to move
            // this page and we are aware of this move. Negating the value
            // tells us not adjust our own move list for the first move
            // detected for this position.

            // FIXME Only add as negative if we are going to observe the move.
            pageRecorder.getUserPageSet().add(soonToBeCreatedUser);

            commit.getEmptyMap().put(iterimAllocation, movable);
        }
    }
    
    /**
     * Add the map of user page moves to the move list. There is no need to
     * record the move addresses in the user page set, since the only move
     * of the page will be the move recored here.
     * 
     * @param commit
     *            Commit map state and move recorder.
     * @param addressMoves
     *            The head of a linked list of move latches.
     */
    private void expandAddress(Commit commit, MoveLatch addressMoves)
    {
        Map<Long, Long> copies = new TreeMap<Long, Long>();
        for (Map.Entry<Long, Movable> entry: commit.getAddressMirrorMap().entrySet())
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
        
        for (Map.Entry<Long, Long> entry: copies.entrySet())
        {
            long addressFromUserPage = entry.getKey();
            long newOrExistingUser = entry.getValue();
            addressMoves.getLast().extend(new MoveLatch(new Move(addressFromUserPage, newOrExistingUser), true));
        }
    }

    private void mirrorUsers(Commit commit, Set<UserPage> setOfMirroredCopyPages)
    {
        for (Map.Entry<Long, Movable> entry: commit.getAddressMirrorMap().entrySet())
        {
            long soonToBeCreatedAddress = entry.getKey();
            long allocation = entry.getValue().getPosition(pager);
            
            Movable movable = commit.getVacuumMap().get(allocation);
            if (movable == null)
            {
                movable = commit.getEmptyMap().get(allocation);
            }
            if (movable == null)
            {
                throw new IllegalStateException();
            }
            
            InterimPage mirrored = pager.getPage(allocation, InterimPage.class, new InterimPage());
            
            UserPage user = pager.getPage(soonToBeCreatedAddress, UserPage.class, new UserPage());
            user.mirror(pager, mirrored, true, dirtyPages);

            setOfMirroredCopyPages.add(user);
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
    
    private void unlockMirrored(Set<UserPage> setOfMirroredPages)
    {
        for (UserPage user : setOfMirroredPages)
        {
            user.unmirror();
        }
        setOfMirroredPages.clear();
    }

    // FIXME Begin line by line documentation here.
    private void tryCommit(MoveLatchList moveList, final Commit commit)
    {
        commit.getUnassignedSet().addAll(pageRecorder.getAllocationPageSet());

        if (commit.getUnassignedSet().size() != 0)
        {
            // First we mate the interim data pages with 
            moveList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    // Consolidate pages by using existing, partially filled
                    // pages to store our new block allocations.
    
                    pager.getFreePageBySize().join(allocPagesBySize, pageRecorder.getUserPageSet(), commit.getVacuumMap(), moveNodeRecorder.getMoveNode());
                    commit.getUnassignedSet().removeAll(commit.getVacuumMap().keySet());
                    
                    // Use free data pages to store the interim pages whose
                    // blocks would not fit on an existing page.
                    
                    pager.newUserPages(commit.getUnassignedSet(), pageRecorder.getUserPageSet(), commit.getEmptyMap(), moveNodeRecorder.getMoveNode());
                }
            });
        }
    
        // If more pages are needed, then we need to extend the user area of
        // the file.
        
        final MoveLatch userMoves = new MoveLatch(false);
    
        if (commit.getUnassignedSet().size() != 0)
        {
            pager.getExpandLock().lock();
            try
            {
                SortedSet<Long> userFromInterimPagesToMove = new TreeSet<Long>();
                
                // Now we can try to move the pages.
                expandUser(moveList, commit, commit.getUnassignedSet().size(), userFromInterimPagesToMove);
                
                // If we have interim pages in use, move them.
                addIterimMoveLatches(moveList, userMoves, userFromInterimPagesToMove);

                asssignAllocations(commit);
            }
            finally
            {
                pager.getExpandLock().unlock();
            }
        }
        
        if (userMoves.getNext() != null)
        {
            moveList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    moveAndUnlatch(userMoves);
                }
            });
        }
        
        final MoveLatch addressMoves = new MoveLatch(true);
        
        if (commit.isAddressExpansion())
        {
            moveList.mutate(new GuardedVoid()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    expandAddress(commit, addressMoves);
                }
            });

            pager.getMoveList().add(addressMoves);
            moveList.skip(addressMoves);
        }

        moveList.mutate(new GuardedVoid()
        {
            public void run(List<MoveLatch> listOfMoveLatches)
            {
                // Write a terminate to end the playback loop. This
                // terminate is the true end of the journal.
    
                journal.write(new Terminate());
    
                // Grab the current position of the journal. This is the
                // actual start of playback.
    
                long beforeVacuum = journal.getJournalPosition();
                
                // Create a vacuum operation for all the vacuums.
                Set<UserPage> setOfMirroredVacuumPages = new HashSet<UserPage>();
                
                // FIXME Do I make sure that mirroring in included before 
                // vacuum in recovery as well?
                // FIXME No. Just make addresses go first. Negative journal.

                for (Map.Entry<Long, Movable> entry: commit.getVacuumMap().entrySet())
                {
                    UserPage user = pager.getPage(entry.getValue().getPosition(pager), UserPage.class, new UserPage());
                    Mirror mirror = user.mirror(pager, null, false, dirtyPages);
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
                
                // Create a vacuum operation for all the vacuums.
                Set<UserPage> setOfMirroredCopyPages = new HashSet<UserPage>();
                mirrorUsers(commit, setOfMirroredCopyPages);

                journalCommits(commit.getVacuumMap());
                journalCommits(commit.getEmptyMap());
               
                for (long position: pageRecorder.getWritePageSet())
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
                Pointer header = pager.getJournalHeaderSet().allocate();
                header.getByteBuffer().putLong(beforeVacuum);
                dirtyPages.flush(header);
                
                Player player = new Player(pager, header, dirtyPages);
                
                // Obtain a journal header and record the head.
                
                // First do the vacuums.
                player.vacuum();
                
                unlockMirrored(setOfMirroredVacuumPages);

                // Then do everything else.
                player.commit();

                unlockMirrored(setOfMirroredCopyPages);
                pager.getAddressLocker().unlock(player.getAddressSet());
                
                if (!commit.isAddressExpansion())
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
                else
                {
                    // FIXME Do I return user pages here?
                }
                
                pager.getFreeInterimPages().free(pageRecorder.getJournalPageSet());
                pager.getFreeInterimPages().free(pageRecorder.getWritePageSet());
            }
        });
        
        MoveLatch iterator = addressMoves;
        while (iterator.getNext() != null && !iterator.getNext().isHead())
        {
            iterator.getNext().unlatch();
            iterator = iterator.getNext();
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
