package com.goodworkalan.pack;

import java.io.File;
import java.io.IOException;
import java.lang.ref.ReferenceQueue;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A container for outstanding <code>Page</code> objects that maps addresses to
 * soft referenced <code>Page</code> objects.
 * 
 * FIXME Javadoc for this class must be done first.
 */
final class Pager
implements Schema
{
    /** The file where this pager writes its contents. */
    private final File file;

    /** An file channel opened on the associated file. */
    private final FileChannel fileChannel;

    /**
     * Wrapper interface to file channel to allow for IO error
     * simulation during testing.
     */
    private final Disk disk;
    
    /**
     * Housekeeping information stored at the head of the file.
     */
    private final Header header;

    /**
     * A map of URIs to addresses of static pages sized at the creation of the
     * <code>Pack</code> in the <code>Schema</code>.
     */
    private final Map<URI, Long> staticPages;

    /**
     * This size of a page.
     */
    private final int pageSize;

    /**
     * Round block allocations to this alignment.
     */
    private final int alignment;

    /**
     * The set of journal header positions stored in a {@link PositionSet} that
     * tracks the availability of a fixed number of header position reference
     * positions and blocks a thread that requests a position when the set is
     * empty.
     */
    private final PositionSet journalHeaders;

    /**
     * The map of weak references to raw pages keyed on the file position of the
     * raw page.
     */
    private final Map<Long, PageReference> pageByPosition;

    /**
     * The queue of weak references to raw pages keyed on the file position of
     * the raw page that is used to remove mappings from the map of pages by
     * position when the raw pages are collected.
     */
    private final ReferenceQueue<RawPage> queue;

    /**
     * The boundary between address pages and user data pages.
     */
    private final Boundary userBoundary;
    
    /**
     * The boundary between user data pages and interim data pages.
     */
    private final Boundary interimBoundary;

    /**
     * A reference to linked list of move nodes used as a prototype for the per
     * mutator move list reference.
     */
    private final MoveLatchList moveLatchList;

    private final SortedSet<Long> setOfAddressPages;
    
    private final Set<Long> setOfReturningAddressPages;
    
    private final AddressLocker addressLocker;
    
    private final BySizeTable freePageBySize;
    
    /**
     * Map of temporary node positions to byte buffers containing the file
     * position value at the temporary node position.
     */
    private final Map<Long, ByteBuffer> temporaryNodes;
    
    private final Map<Long, Long> temporaries;

    private final FreeSet setOfFreeUserPages;

    /**
     * A sorted set of of free interim pages sorted in descending order so that
     * we can quickly obtain the last free interim page within interim page
     * space.
     * <p>
     * This set of free interim pages guards against overwrites by a simple
     * method. If the position is in the set of free interim pages, then it is
     * free, if not it is not free. System pages must be allocated while the
     * move lock is locked for reading, or locked for writing in the case of
     * removing free pages from the start of the interim page area when the user
     * area expands.
     * <p>
     * Question: Can't an interim page allocated from the set of free pages be
     * moved while we are first writing to it?
     * <p>
     * Answer: No, because the moving mutator will have to add the moves to the
     * move list before it can move the pages. Adding to move list requires an
     * exclusive lock on the move list.
     * <p>
     * Remember: Only one mutator can move pages in the interim area at a time.
     */
    private final FreeSet freeInterimPages;

    /**
     * A read/write lock that coordinates rewind of area boundaries and the
     * wilderness.
     * <p>
     * The compact lock locks the entire file exclusively and block any other
     * moves or commits. Ordinary commits can run in parallel so long as blocks
     * are moved forward and not backward in in the file.
     */
    private final ReadWriteLock compactLock;

    /**
     * A mutex to ensure that only one mutator at a time is moving pages in the
     * interim page area.
     */
    private final Object expandMutex;
    
    public Pager(File file, FileChannel fileChannel, Disk disk, Header header, Map<URI, Long> mapOfStaticPages, SortedSet<Long> setOfAddressPages,
        long dataBoundary, long interimBoundary, Map<Long, ByteBuffer> temporaryNodes)
    {
        this.file = file;
        this.fileChannel = fileChannel;
        this.disk = disk;
        this.header = header;
        this.alignment = header.getAlignment();
        this.pageSize = header.getPageSize();
        this.userBoundary = new Boundary(pageSize, dataBoundary);
        this.interimBoundary = new Boundary(pageSize, interimBoundary);
        this.pageByPosition = new HashMap<Long, PageReference>();
        this.freePageBySize = new BySizeTable(pageSize, alignment);
        this.staticPages = mapOfStaticPages;
        this.setOfFreeUserPages = new FreeSet();
        this.freeInterimPages = new FreeSet();
        this.queue = new ReferenceQueue<RawPage>();
        this.compactLock = new ReentrantReadWriteLock();
        this.expandMutex = new Object();
        this.moveLatchList = new MoveLatchList();
        this.journalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.getInternalJournalCount());
        this.setOfAddressPages = setOfAddressPages;
        this.setOfReturningAddressPages = new HashSet<Long>();
        this.temporaryNodes = temporaryNodes;
        this.temporaries = temporaries(temporaryNodes);
        this.addressLocker = new AddressLocker();
    }

    /**
     * Create a map of temporary block addresses to the file position of their
     * temporary reference node.
     * 
     * @param temporaryNodes
     *            Map of temporary node positions to byte buffers containing the
     *            file position value at the temporary node position.
     * @return A map of temporary block addresses to temporary node reference
     *         positions.
     */
    private static Map<Long, Long> temporaries(Map<Long, ByteBuffer> temporaryNodes)
    {
        Map<Long, Long> temporaries = new HashMap<Long, Long>();
        for (Map.Entry<Long, ByteBuffer> entry : temporaryNodes.entrySet())
        {
            ByteBuffer bytes = entry.getValue();
            long address = bytes.getLong();
            if (address != 0L)
            {
                temporaries.put(address, entry.getKey());
            }
        }
        return temporaries;
    }
    
    /**
     * Return the file associated with this pack.
     * 
     * @return The pack file.
     */
    public File getFile()
    {
        return file;
    }

    /**
     * Return the file channel open on the underlying file.
     * 
     * @return The open file channel.
     */
    public FileChannel getFileChannel()
    {
        return fileChannel;
    }

    /**
     * Return the disk object used to read and write to the file channel. The
     * disk is an class that can be overridden to generate I/O errors during
     * testing.
     * 
     * @return The disk.
     */
    public Disk getDisk()
    {
        return disk;
    }

    /**
     * Get the size of all underlying pages managed by this pager.
     * 
     * @return The page size.
     */
    public int getPageSize()
    {
        return pageSize;
    }
    
    /**
     * Return the alignment to which all block allocations are rounded.
     * 
     * @return The block alignment.
     */
    public int getAlignment()
    {
        return alignment;
    }
    
    public long getStaticPageAddress(URI uri)
    {
        return staticPages.get(uri);
    }
    
    public boolean isStaticPageAddress(long address)
    {
        return staticPages.containsValue(address);
    }

    public PositionSet getJournalHeaders()
    {
        return journalHeaders;
    }

    public long getFirstAddressPageStart()
    {
        return header.getFirstAddressPageStart();
    }

    /**
     * Get the boundary between address pages and user data pages.
     *
     * @return The boundary between address pages and user data pages.
     */
    public Boundary getUserBoundary()
    {
        return userBoundary;
    }

    /**
     * Get the boundary between user pages and interim pages.
     *
     * @return The boundary between user pages and interim pages.
     */
    public Boundary getInterimBoundary()
    {
        return interimBoundary;
    }

    public MoveLatchList getMoveLatchList()
    {
        return moveLatchList;
    }
    
    public AddressLocker getAddressLocker()
    {
        return addressLocker;
    }
    
    public BySizeTable getFreePageBySize()
    {
        return freePageBySize;
    }
    
    public FreeSet getFreeUserPages()
    {
        return setOfFreeUserPages;
    }
    
    public FreeSet getFreeInterimPages()
    {
        return freeInterimPages;
    }

    public Object getExpandMutex()
    {
        return expandMutex;
    }
    
    public ReadWriteLock getCompactLock()
    {
        return compactLock;
    }
    
    private synchronized void collect()
    {
        Positionable positionable = null;
        while ((positionable = (Positionable) queue.poll()) != null)
        {
            pageByPosition.remove(positionable.getPosition());
        }
    }

    private long fromWilderness()
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);

        bytes.putLong(0L); // Checksum.
        bytes.putInt(0); // Is system page.

        bytes.clear();

        long position;

        synchronized (disk)
        {
            try
            {
                position = disk.size(fileChannel);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_SIZE, e);
            }

            try
            {
                disk.write(fileChannel, bytes, position);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_WRITE, e);
            }

            try
            {
                if (disk.size(fileChannel) % 1024 != 0)
                {
                    throw new PackException(Pack.ERROR_FILE_SIZE);
                }
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_SIZE, e);
            }
        }

        return position;
    }

    private void addPageByPosition(RawPage page)
    {
        PageReference intended = new PageReference(page, queue);
        PageReference existing = pageByPosition.get(intended.getPosition());
        if (existing != null)
        {
            existing.enqueue();
            collect();
        }
        pageByPosition.put(intended.getPosition(), intended);
    }
    
    /**
     * Allocate a new interim position that is initialized by the
     * specified page strategy.
     * <p>
     * This method can only be called from within one of the
     * <code>MoveList.mutate</code> methods. A page obtained from the set of
     * free interim pages will not be moved while the move list is locked
     * shared. 
     * 
     * @param <T>
     *            The page strategy for the position.
     * @param page
     *            An instance of the page strategy that will initialize
     *            the page at the position.
     * @param dirtyPages
     *            A map of dirty pages.
     * @return A new interim page.
     */
    public <T extends Page> T newInterimPage(T page, DirtyPageSet dirtyPages)
    {
        // We pull from the end of the interim space to take pressure of of
        // the durable pages, which are more than likely multiply in number
        // and move interim pages out of the way. We could change the order
        // of the interim page set, so that we choose free interim pages
        // from the front of the interim page space, if we want to rewind
        // the interim page space and shrink the file more frequently.
    
        long position = getFreeInterimPages().allocate();
    
        // If we do not have a free interim page available, we will obtain
        // create one out of the wilderness.
    
        if (position == 0L)
        {
            position = fromWilderness();
        }
    
        RawPage rawPage = new RawPage(this, position);
    
        page.create(rawPage, dirtyPages);
    
        synchronized (pageByPosition)
        {
            addPageByPosition(rawPage);
        }
    
        return page;
    }

    /**
     * Return an interim page for use as a move destination.
     * <p>
     * Question: How do we ensure that free interim pages do not slip into
     * the user data page section? That is, how do we ensure that we're
     * not moving an interim page to a spot that also needs to move?
     * <p>
     * Simple. We gather all the pages that need to move first. Then we
     * assign blank pages only to the pages that are in use and need to
     * move. See <code>tryMove</code> for more discussion.
     * 
     * @return A blank position in the interim area that for use as the
     *         target of a move.
     */
    public long newBlankInterimPage()
    {
        long position = getFreeInterimPages().allocate();
        if (position == 0L)
        {
            position = fromWilderness();
        }
        return position;
    }


    private RawPage getPageByPosition(long position)
    {
        RawPage page = null;
        Long boxPosition = new Long(position);
        PageReference chunkReference = pageByPosition.get(boxPosition);
        if (chunkReference != null)
        {
            page = chunkReference.get();
        }
        return page;
    }

    public <P extends Page> P getPage(long position, Class<P> pageClass, P page)
    {
        position = (long) Math.floor(position - (position % pageSize));
        RawPage rawPage = new RawPage(this, position);
        synchronized (rawPage)
        {
            RawPage found = null;
            synchronized (pageByPosition)
            {
                found = getPageByPosition(position);
                if (found == null)
                {
                    addPageByPosition(rawPage);
                }
            }
            if (found == null)
            {
                page.load(rawPage);
            }
            else
            {
                rawPage = found;
            }
        }
        synchronized (rawPage)
        {
            if (!page.getClass().isAssignableFrom(rawPage.getPage().getClass()))
            {
                if (!rawPage.getPage().getClass().isAssignableFrom(page.getClass()))
                {
                    throw new IllegalStateException();
                }
                page.load(rawPage);
            }
        }
        return pageClass.cast(rawPage.getPage());
    }

    public <P extends Page> P setPage(long position, Class<P> pageClass, P page, DirtyPageSet dirtyPages, boolean extant)
    {
        position =  position / pageSize * pageSize;
        RawPage rawPage = new RawPage(this, position);

        synchronized (pageByPosition)
        {
            RawPage existing = removePageByPosition(position);
            if (existing != null)
            {
                if (!extant)
                {
                    throw new IllegalStateException();
                }
                // Not sure about this. Maybe lock on existing?
                synchronized (existing)
                {
                    page.create(existing, dirtyPages);
                }
            }
            else
            {
                page.create(rawPage, dirtyPages);
                addPageByPosition(rawPage);
            }
        }

        return pageClass.cast(rawPage.getPage());
    }

    /**
     * Try to get an address page from the set of available address pages or
     * create address pages by moving user pages if there are none available or
     * outstanding.
     * <p>
     * The caller can allocate one and only address from address page
     * returned. It must then return the address page to the pager.
     * 
     * @param lastSelected
     *            The last selected address page, which we will try to return if
     *            available.
     * 
     * @return An address page with at least one free position.
     */
    private AddressPage getOrCreateAddressPage(long lastSelected)
    {
        // Lock on the set of address pages, which protects the set of
        // address pages and the set of returning address pages.
        
        synchronized (setOfAddressPages)
        {
            long position = 0L;         // The position of the new address page.

            // If there are no address pages in the pager that have one
            // or more free addresses and there are no address pages
            // outstanding that have two or more free addresses, then we
            // need to allocate more address pages and try again.
            
            if (setOfAddressPages.size() == 0 && setOfReturningAddressPages.size() == 0)
            {
                // Create a mutator to move the user page immediately
                // following the address page region  to a new user
                // page.

                final Pager pack = this;
                final PageRecorder pageRecorder = new PageRecorder();
                final MoveLatchList listOfMoves = new MoveLatchList(pageRecorder, getMoveLatchList());
                Mutator mutator = listOfMoves.mutate(new Guarded<Mutator>()
                {
                    public Mutator run(List<MoveLatch> listOfMoveLatches)
                    {
                        MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                        DirtyPageSet dirtyPages = new DirtyPageSet(pack, 16);
                        Journal journal = new Journal(pack, moveNodeRecorder, pageRecorder, dirtyPages);
                        return new Mutator(pack, listOfMoves, moveNodeRecorder, pageRecorder, journal,dirtyPages);
                    }
                });

                // See the source of Mutator.newAddressPage.

                position = mutator.newAddressPages(1).first();
            }
            else
            {
                // If we can return the last selected address page,
                // let's. It will help with locality of reference.

                if (setOfAddressPages.contains(lastSelected))
                {
                    position = lastSelected;
                }
                else if (setOfAddressPages.size() != 0)
                {
                    position = setOfAddressPages.first();
                }
                else
                {
                    // We are here because our set of returning address
                    // pages is not empty, meaning that a Mutator will
                    // be returning an address page that has some space
                    // left. We need to wait for it.
                    
                    try
                    {
                        setOfAddressPages.wait();
                    }
                    catch (InterruptedException e)
                    {
                    }

                    // When it arrives, we'll return null indicating we
                    // need to try again.

                    return null;
                }

                // Remove the address page from the set of address
                // pages available for allocation.

                setOfAddressPages.remove(position);
            }

            // Get the address page.

            AddressPage addressPage = getPage(position, AddressPage.class, new AddressPage());

            // If the address page has two or more addresses available,
            // then we add it to the set of returning address pages, the
            // address pages that have space, but are currently in use,
            // so we should wait for them.

            if (addressPage.getFreeCount() > 1)
            {
                setOfReturningAddressPages.add(position);
            }

            // Return the address page.

            return addressPage;
        }
    }

    public AddressPage getAddressPage(long lastSelected)
    {
        for (;;)
        {
            AddressPage addressPage = getOrCreateAddressPage(lastSelected);
            if (addressPage != null)
            {
                return addressPage;
            }
        }
    }
    
    public void returnAddressPage(AddressPage addressPage)
    {
        long position = addressPage.getRawPage().getPosition();
        synchronized (setOfAddressPages)
        {
            if (setOfReturningAddressPages.remove(position))
            {
                setOfAddressPages.add(position);
                setOfAddressPages.notifyAll();
            }
        }
    }

    public void addAddressPage(AddressPage addressPage)
    {
        // TODO Convince yourself that this works. That you're not really
        // using the count, but rather the check in and checkout. You
        // could be checking the available, but you'll only determine
        // not to come back, in fact, check in only one place, the the
        // tryGetAddressPage method.
        long position = addressPage.getRawPage().getPosition();
        synchronized (setOfAddressPages)
        {
            if (!setOfReturningAddressPages.contains(position)
                && !setOfAddressPages.contains(position))
            {
                setOfAddressPages.add(position);
                setOfAddressPages.notifyAll();
            }
        }
    }

    /**
     * Create a journal entry that will write a temporary node reference FIXME a temporary node for the given temporary block address.
     */
    public Temporary getTemporary(long address)
    {
        Temporary temporary = null;
        synchronized (temporaryNodes)
        {
            BUFFERS: for (;;)
            {
                Map.Entry<Long, ByteBuffer> last = null;
                for (Map.Entry<Long, ByteBuffer> entry : temporaryNodes.entrySet())
                {
                    ByteBuffer bytes = entry.getValue();
                    if (bytes.getLong(Pack.ADDRESS_SIZE) == 0L)
                    {
                        last = entry;
                    }
                    else if (bytes.getLong(0) == 0L)
                    {
                        temporaries.put(address, entry.getKey());
                        bytes.putLong(0, Long.MAX_VALUE);
                        temporary = new Temporary(address, entry.getKey());
                        break BUFFERS;
                    }
                }
                final Pager pack = this;
                final PageRecorder pageRecorder = new PageRecorder();
                final MoveLatchList listOfMoves = new MoveLatchList(pageRecorder, getMoveLatchList());
                Mutator mutator = listOfMoves.mutate(new Guarded<Mutator>()
                {
                    public Mutator run(List<MoveLatch> listOfMoveLatches)
                    {
                        MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                        DirtyPageSet dirtyPages = new DirtyPageSet(pack, 16);
                        Journal journal = new Journal(pack, moveNodeRecorder, pageRecorder, dirtyPages);
                        return new Mutator(pack, listOfMoves, moveNodeRecorder, pageRecorder, journal,dirtyPages);
                    }
                });
                
                long next = mutator.allocate(Pack.ADDRESS_SIZE * 2);
                
                ByteBuffer bytes = mutator.read(next);
                while (bytes.remaining() != 0)
                {
                    bytes.putLong(0L);
                }
                bytes.flip();
                
                mutator.write(next, bytes);

                last.getValue().clear();
                last.getValue().putLong(Pack.ADDRESS_SIZE, next);
                
                mutator.write(last.getKey(), last.getValue());
                
                mutator.commit();
                
                temporaryNodes.put(next, bytes);
            }
        }

        return temporary;
    }
    
    /**
     * Set the temporary node at the given temporary node position to reference
     * the given block address.
     *
     * @param address The temporary block address.
     * @param temporary The position of the temporary reference node.
     * @param dirtyPages The set of dirty pages.
     */
    public void setTemporary(long address, long temporary, DirtyPageSet dirtyPages)
    {
        synchronized (temporaryNodes)
        {
            ByteBuffer bytes = temporaryNodes.get(temporary);
            bytes.putLong(0, address);
            bytes.clear();
            AddressPage addresses = getPage(temporary, AddressPage.class, new AddressPage());
            long lastPosition = 0L;
            for (;;)
            {
                long position = addresses.dereference(temporary);
                if (lastPosition == position)
                {
                    throw new IllegalStateException();
                }
                UserPage user = getPage(position, UserPage.class, new UserPage());
                synchronized (user.getRawPage())
                {
                    if (user.write(temporary, bytes, dirtyPages))
                    {
                        break;
                    }
                }
                lastPosition = position;
            }
        }
    }

    /**
     * Free a temporary block reference, setting the block reference to zero,
     * making it available for use to reference another temporary block.
     * 
     * @param address
     *            The address of the temporary block.
     * @param dirtyPages
     *            The dirty page set.
     */
    public void freeTemporary(long address, DirtyPageSet dirtyPages)
    {
        synchronized (temporaryNodes)
        {
            Long temporary = temporaries.get(address);
            if (temporary != null)
            {
                setTemporary(0L, temporary, dirtyPages);
            }
        }
    }

    /**
     * Return a temporary block reference to the pool of temporary block
     * references as the result of a rollback of a commit.
     * 
     * @param address The address of the temporary block.
     * @param temporary The temporary block 
     */
    public void rollbackTemporary(long address, long temporary)
    {
        synchronized (temporaryNodes)
        {
            temporaries.remove(address);
            temporaryNodes.get(temporary).putLong(0, 0L);
        }
    }

    public void newUserPages(SortedSet<Long> setOfSourcePages, Set<Long> setOfDataPages, Map<Long, Movable> mapOfPages, MoveNode moveNode)
    {
        while (setOfSourcePages.size() != 0)
        {
            long position = setOfFreeUserPages.allocate();
            if (position == 0L)
            {
                break;
            }
            setOfDataPages.add(position);
            long source = setOfSourcePages.first();
            setOfSourcePages.remove(source);
            mapOfPages.put(source, new Movable(moveNode, position, 0));
        }
    }

    public void returnUserPage(BlockPage blocks)
    {
        // TODO Is it not the case that the block changes can mutated by virtue
        // of a deallocation operation?
        if (blocks.getCount() == 0)
        {
            setOfFreeUserPages.free(blocks.getRawPage().getPosition());
        }
        else if (blocks.getRemaining() > getAlignment())
        {
            getFreePageBySize().add(blocks);
        }
    }

    /**
     * Remove a raw page from the map of pages by position. If the page exists
     * in the map, The page is completly removed by enqueuing the weak page
     * reference and running <code>collect</code>.
     * 
     * @param position
     *            The position of the page to remove.
     * @return The page currently mapped to the position or null if no page is
     *         mapped.
     */
    private RawPage removePageByPosition(long position)
    {
        PageReference existing = pageByPosition.get(new Long(position));
        RawPage p = null;
        if (existing != null)
        {
            p = existing.get();
            existing.enqueue();
            collect();
        }
        return p;
    }

    /**
     * Relocate a page in the pager by removing it from the map of pages by
     * position at the given from position and adding it at the given to
     * position. This only moves the <code>RawPage</code> in the pager, it does
     * not copy the page to the new position in the underlying file.
     * 
     * @param from
     *            The position to move from.
     * @param to
     *            The position to move to.
     */
    public void relocate(long from, long to)
    {
        synchronized (pageByPosition)
        {
            RawPage position = removePageByPosition(from);
            if (position != null)
            {
                assert to == position.getPosition();
                addPageByPosition(position);
            }
        }
    }

    /**
     * Return a file position based on the given file position adjusted by the
     * linked list of page moves appended to the given move node. The adjustment
     * will skip the number of move nodes given by skip.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param moveNode
     *            The head of a list of move nodes.
     * @param position
     *            The file position to track.
     * @param skip
     *            The number of moves to skip (0 or 1).
     * @return The file position adjusted by the recorded page moves.
     */
    public long adjust(MoveNode moveNode, long position, int skip)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        while (moveNode.getNext() != null)
        {
            moveNode = moveNode.getNext();
            Move move = moveNode.getMove();
            if (move.getFrom() == position)
            {
                if (skip == 0)
                {
                    position = move.getTo();
                }
                else
                {
                    skip--;
                }
            }
        }
        return position + offset;
    }

    /**
     * Return a file position based on the given file position adjusted by page
     * moves in the given list of move node.
     * <p>
     * The adjustment will account for offset into the page position. This is
     * necessary for next operations in journals, which may jump to any
     * operation in a journal, which may be at any location in a page.
     * 
     * @param moveList
     *            The list of page moves.
     * @param position
     *            The file position to track.
     * @return The file position adjusted by the recorded page moves.
     */
    public long adjust(List<Move> moveList, long position)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        for (Move move: moveList)
        {
            if (move.getFrom() == position)
            {
                position = move.getTo();
            }
        }
        return position + offset;
    }

    /**
     * Close the pager writing out the region boudnaries and the soft shutdown
     * flag in the header and variable pager state to a region at the end of the
     * file. The variable data includes address pages with addresses remaining,
     * empty user pages, and user pages with space remaining. The variable data
     * is positioned at the location indicated by the user to interim boundary.
     * <p>
     * Close will wait for any concurrent commits to complete.
     */
    public void close()
    {
        // Grab the exclusive compact lock, which will wait for any concurrent
        // commits to complete.

        getCompactLock().writeLock().lock();

        try
        {
            // Write the set of address pages, the set of empty user pages and
            // the set of pages with space remaining to a byte buffer.

            int size = 0;
            
            size += Pack.COUNT_SIZE + setOfAddressPages.size() * Pack.POSITION_SIZE;
            size += Pack.COUNT_SIZE + (setOfFreeUserPages.size() + getFreePageBySize().getSize()) * Pack.POSITION_SIZE;
            
            ByteBuffer reopen = ByteBuffer.allocateDirect(size);
            
            reopen.putInt(setOfAddressPages.size());
            for (long position: setOfAddressPages)
            {
                reopen.putLong(position);
            }
           
            reopen.putInt(setOfFreeUserPages.size() + getFreePageBySize().getSize());
            for (long position: setOfFreeUserPages)
            {
                reopen.putLong(position);
            }
            for (long position: getFreePageBySize())
            {
                reopen.putLong(position);
            }
            
            reopen.flip();

            // Write the variable data at the interim page positions.
            
            try
            {
                disk.write(fileChannel, reopen, getInterimBoundary().getPosition());
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_WRITE, e);
            }

            // Write the boundaries and soft shutdown flag.
            
            try
            {
                disk.truncate(fileChannel, getInterimBoundary().getPosition() + reopen.capacity());
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_TRUNCATE, e);
            }
            
            header.setDataBoundary(getUserBoundary().getPosition());
            header.setOpenBoundary(getInterimBoundary().getPosition());
    
            header.setShutdown(Pack.SOFT_SHUTDOWN);
            try
            {
                header.write(disk, fileChannel);
                disk.close(fileChannel);
            }
            catch (IOException e)
            {
                throw new PackException(Pack.ERROR_IO_CLOSE, e);
            }
        }
        finally
        {
            getCompactLock().writeLock().unlock();
        }
    }
}

/* vim: set tw=80 : */
