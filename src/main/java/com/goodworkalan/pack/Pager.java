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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * A container for outstanding <code>Page</code> objects that maps
 * addresses to soft referenced * <code>Page</code> objects.
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
     * A map of URIs to addresses of static pages sized at the creation
     * of the <code>Pack</code> in the <code>Schema</code>.
     */
    private final Map<URI, Long> mapOfStaticPages;

    /**
     * This size of a page.
     */
    private final int pageSize;

    /**
     * Round allocations to this alignment.
     */
    private final int alignment;
    
    private final PositionSet setOfJournalHeaders;

    private final Map<Long, PageReference> mapOfPagesByPosition;

    private final ReferenceQueue<RawPage> queue;

    private final Boundary userBoundary;
    
    private final Boundary interimBoundary;
        
    private final MoveList listOfMoves;

    private final SortedSet<Long> setOfAddressPages;
    
    private final Set<Long> setOfReturningAddressPages;
    
    private final AddressLocker addressLocker;
    
    public final BySizeTable pagesBySize_;
    
    private final Map<Long, ByteBuffer> mapOfTemporaryNodes;
    
    private final Map<Long, Long> mapOfTemporaries;

    private final FreeSet setOfFreeUserPages;

    /**
     * A sorted set of of free interim pages sorted in descending order so
     * that we can quickly obtain the last free interim page within interim
     * page space.
     * <p>
     * This set of free interim pages guards against overwrites by a simple
     * method. If the position is in the set of free interim pages, then it
     * is free, if not it is not free. System pages must be allocated while
     * the move lock is locked for reading, or locked for writing in the
     * case of removing free pages from the start of the interim page area
     * when the user area expands.
     * <p>
     * Question: Can't an interim page allocated from the set of free pages
     * be moved while we are first writing to it?
     * <p>
     * Answer: No, because the moving mutator will have to add the moves to
     * the move list before it can move the pages. Adding to move list
     * requires an exclusive lock on the move list.
     * <p>
     * Remember: Only one mutator can move pages in the interim area at a
     * time.
     */
    private final FreeSet setOfFreeInterimPages;
    
    /**
     * A read/write lock that coordinates rewind of area boundaries and the
     * wilderness. 
     * <p>
     * The compact lock locks the entire file exclusively and block any
     * other moves or commits. Ordinary commits can run in parallel so long
     * as blocks are moved forward and not backward in in the file.
     */
    private final ReadWriteLock compactLock;
    
    /**
     * A lock to ensure that only one mutator at a time is moving pages in
     * the interim page area.
     */
    private final Lock expandLock;
    
    public Pager(File file, FileChannel fileChannel, Disk disk, Header header, Map<URI, Long> mapOfStaticPages, SortedSet<Long> setOfAddressPages,
        long dataBoundary, long interimBoundary, Map<Long, ByteBuffer> mapOfTemporaryNodes)
    {
        this.file = file;
        this.fileChannel = fileChannel;
        this.disk = disk;
        this.header = header;
        this.alignment = header.getAlignment();
        this.pageSize = header.getPageSize();
        this.userBoundary = new Boundary(pageSize, dataBoundary);
        this.interimBoundary = new Boundary(pageSize, interimBoundary);
        this.mapOfPagesByPosition = new HashMap<Long, PageReference>();
        this.pagesBySize_ = new BySizeTable(pageSize, alignment);
        this.mapOfStaticPages = mapOfStaticPages;
        this.setOfFreeUserPages = new FreeSet();
        this.setOfFreeInterimPages = new FreeSet();
        this.queue = new ReferenceQueue<RawPage>();
        this.compactLock = new ReentrantReadWriteLock();
        this.expandLock = new ReentrantLock();
        this.listOfMoves = new MoveList();
        this.setOfJournalHeaders = new PositionSet(Pack.FILE_HEADER_SIZE, header.getInternalJournalCount());
        this.setOfAddressPages = setOfAddressPages;
        this.setOfReturningAddressPages = new HashSet<Long>();
        this.mapOfTemporaryNodes = mapOfTemporaryNodes;
        this.mapOfTemporaries = mapOfTemporaries(mapOfTemporaryNodes);
        this.addressLocker = new AddressPageAddressLocker();
    }
    
    private Map<Long, Long> mapOfTemporaries(Map<Long, ByteBuffer> mapOfTemporaryNodes)
    {
        Map<Long, Long> mapOfTemporaries = new HashMap<Long, Long>();
        for (Map.Entry<Long, ByteBuffer> entry : mapOfTemporaryNodes.entrySet())
        {
            ByteBuffer bytes = entry.getValue();
            long address = bytes.getLong();
            if (address != 0L)
            {
                mapOfTemporaries.put(address, entry.getKey());
            }
        }
        return mapOfTemporaries;
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
     * Return the 
     * @return
     */
    public FileChannel getFileChannel()
    {
        return fileChannel;
    }

    public Disk getDisk()
    {
        return disk;
    }

    public int getPageSize()
    {
        return pageSize;
    }

    public int getAlignment()
    {
        return alignment;
    }
    
    public long getStaticPageAddress(URI uri)
    {
        return mapOfStaticPages.get(uri);
    }
    
    public boolean isStaticPageAddress(long address)
    {
        return mapOfStaticPages.containsValue(address);
    }

    public PositionSet getJournalHeaderSet()
    {
        return setOfJournalHeaders;
    }

    public long getFirstAddressPageStart()
    {
        return header.getFirstAddressPageStart();
    }

    public Boundary getUserBoundary()
    {
        return userBoundary;
    }

    public Boundary getInterimBoundary()
    {
        return interimBoundary;
    }

    public MoveList getMoveList()
    {
        return listOfMoves;
    }
    
    public AddressLocker getAddressLocker()
    {
        return addressLocker;
    }
    
    public BySizeTable getFreePageBySize()
    {
        return pagesBySize_;
    }
    
    public FreeSet getFreeUserPages()
    {
        return setOfFreeUserPages;
    }
    
    public FreeSet getFreeInterimPages()
    {
        return setOfFreeInterimPages;
    }

    public Lock getExpandLock()
    {
        return expandLock;
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
            mapOfPagesByPosition.remove(positionable.getPosition());
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
                throw new Danger(Pack.ERROR_IO_SIZE, e);
            }

            try
            {
                disk.write(fileChannel, bytes, position);
            }
            catch (IOException e)
            {
                throw new Danger(Pack.ERROR_IO_WRITE, e);
            }

            try
            {
                if (disk.size(fileChannel) % 1024 != 0)
                {
                    throw new Danger(Pack.ERROR_FILE_SIZE);
                }
            }
            catch (IOException e)
            {
                throw new Danger(Pack.ERROR_IO_SIZE, e);
            }
        }

        return position;
    }

    private void addPageByPosition(RawPage page)
    {
        PageReference intended = new PageReference(page, queue);
        PageReference existing = mapOfPagesByPosition.get(intended.getPosition());
        if (existing != null)
        {
            existing.enqueue();
            collect();
        }
        mapOfPagesByPosition.put(intended.getPosition(), intended);
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
    
        synchronized (mapOfPagesByPosition)
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
        PageReference chunkReference = mapOfPagesByPosition.get(boxPosition);
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
            synchronized (mapOfPagesByPosition)
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

        synchronized (mapOfPagesByPosition)
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
    
    private AddressPage tryGetAddressPage(long lastSelected)
    {
        synchronized (setOfAddressPages)
        {
            long position = 0L;
            if (setOfAddressPages.size() == 0 && setOfReturningAddressPages.size() == 0)
            {
                final Pager pack = this;
                final PageRecorder pageRecorder = new PageRecorder();
                final MoveList listOfMoves = new MoveList(pageRecorder, getMoveList());
                Mutator mutator = listOfMoves.mutate(new GuardedReturnable<Mutator>()
                {
                    public Mutator run(List<MoveLatch> listOfMoveLatches)
                    {
                        MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder();
                        DirtyPageSet dirtyPages = new DirtyPageSet(pack, 16);
                        Journal journal = new Journal(pack, moveNodeRecorder, pageRecorder, dirtyPages);
                        return new Mutator(pack, listOfMoves, moveNodeRecorder, pageRecorder, journal,dirtyPages);
                    }
                });
                position = mutator.newAddressPage().first();
            }
            else
            {
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
                    try
                    {
                        setOfAddressPages.wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
                    return null;
                }
                setOfAddressPages.remove(position);
            }
            AddressPage addressPage = getPage(position, AddressPage.class, new AddressPage());
            if (addressPage.getFreeCount() > 1)
            {
                setOfReturningAddressPages.add(position);
            }
            return addressPage;
        }
    }

    public AddressPage getAddressPage(long lastSelected)
    {
        for (;;)
        {
            AddressPage addressPage = tryGetAddressPage(lastSelected);
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
        // FIXME Convince yourself that this works. That you're not really
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

    public Temporary getTemporary(long address)
    {
        Temporary temporary = null;
        synchronized (mapOfTemporaryNodes)
        {
            BUFFERS: for (;;)
            {
                Map.Entry<Long, ByteBuffer> last = null;
                for (Map.Entry<Long, ByteBuffer> entry : mapOfTemporaryNodes.entrySet())
                {
                    ByteBuffer bytes = entry.getValue();
                    if (bytes.getLong(Pack.ADDRESS_SIZE) == 0L)
                    {
                        last = entry;
                    }
                    else if (bytes.getLong(0) == 0L)
                    {
                        mapOfTemporaries.put(address, entry.getKey());
                        bytes.putLong(0, Long.MAX_VALUE);
                        temporary = new Temporary(address, entry.getKey());
                        break BUFFERS;
                    }
                }
                final Pager pack = this;
                final PageRecorder pageRecorder = new PageRecorder();
                final MoveList listOfMoves = new MoveList(pageRecorder, getMoveList());
                Mutator mutator = listOfMoves.mutate(new GuardedReturnable<Mutator>()
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
                
                mapOfTemporaryNodes.put(next, bytes);
            }
        }

        return temporary;
    }
    
    public void setTemporary(long address, long temporary, DirtyPageSet dirtyPages)
    {
        synchronized (mapOfTemporaryNodes)
        {
            ByteBuffer bytes = mapOfTemporaryNodes.get(temporary);
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

    public void freeTemporary(long address, DirtyPageSet dirtyPages)
    {
        synchronized (mapOfTemporaryNodes)
        {
            Long temporary = mapOfTemporaries.get(address);
            if (temporary != null)
            {
                setTemporary(0L, temporary, dirtyPages);
            }
        }
    }

    public void freeTemporary(long address, long temporary)
    {
        synchronized (mapOfTemporaryNodes)
        {
            mapOfTemporaries.remove(address);
            mapOfTemporaryNodes.get(temporary).putLong(0, 0L);
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
        // FIXME Is it not the case that the block changes can mutated
        // by virtue of a deallocation operation?
        if (blocks.getCount() == 0)
        {
            setOfFreeUserPages.free(blocks.getRawPage().getPosition());
        }
        else if (blocks.getRemaining() > getAlignment())
        {
            getFreePageBySize().add(blocks);
        }
    }

    private RawPage removePageByPosition(long position)
    {
        PageReference existing = mapOfPagesByPosition.get(new Long(position));
        RawPage p = null;
        if (existing != null)
        {
            p = existing.get();
            existing.enqueue();
            collect();
        }
        return p;
    }

    public void relocate(long from, long to)
    {
        synchronized (mapOfPagesByPosition)
        {
            RawPage position = removePageByPosition(from);
            if (position != null)
            {
                assert to == position.getPosition();
                addPageByPosition(position);
            }
        }
    }
    
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

    public long adjust(List<Move> listOfMoves, long position)
    {
        int offset = (int) (position % pageSize);
        position = position - offset;
        for (Move move: listOfMoves)
        {
            if (move.getFrom() == position)
            {
                position = move.getTo();
            }
        }
        return position + offset;
    }

    public void close()
    {
        getCompactLock().writeLock().lock();
        try
        {
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
            
            try
            {
                disk.write(fileChannel, reopen, getInterimBoundary().getPosition());
            }
            catch (IOException e)
            {
                throw new Danger(Pack.ERROR_IO_WRITE, e);
            }
            
            try
            {
                disk.truncate(fileChannel, getInterimBoundary().getPosition() + reopen.capacity());
            }
            catch (IOException e)
            {
                throw new Danger(Pack.ERROR_IO_TRUNCATE, e);
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
                throw new Danger(Pack.ERROR_IO_CLOSE, e);
            }
        }
        finally
        {
            getCompactLock().writeLock().unlock();
        }
    }
}
