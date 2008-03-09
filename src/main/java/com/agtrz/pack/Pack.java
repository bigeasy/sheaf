/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class Pack
{
    private final static int FLAG_SIZE = 2;

    private final static int COUNT_SIZE = 4;

    private final static int POSITION_SIZE = 8;

    private final static int CHECKSUM_SIZE = 8;

    private final static int ADDRESS_SIZE = 8;

    private final static int FILE_HEADER_SIZE = COUNT_SIZE * 4 + ADDRESS_SIZE;

    private final static int DATA_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    private final static int BLOCK_HEADER_SIZE = POSITION_SIZE + COUNT_SIZE;

    private final static short ALLOCATE = 1;

    private final static short WRITE = 2;

    private final static short FREE = 3;

    private final static short NEXT_PAGE = 4;

    private final static short COMMIT = 5;

    private final static short VACUUM = 6;

    private final static short SHIFT = 7;

    private final static int NEXT_PAGE_SIZE = FLAG_SIZE + ADDRESS_SIZE;

    private final static int ADDRESS_PAGE_HEADER_SIZE = CHECKSUM_SIZE + POSITION_SIZE;

    private final static int RESERVATION_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    private final static NullAllocator NULL_ALLOCATOR = new NullAllocator();

    private final Pager pager;
    
    private final PageLocker pageLocker;

    public Pack(Pager pager)
    {
        this.pager = pager;
        this.pageLocker = new PageLocker(17);
    }

    /**
     * Create an object that can inspect and alter the contents of this pack.
     * 
     * @return A new {@link Pack.Mutator}.
     */
    public Mutator mutate()
    {
        DirtyPageMap pages = new DirtyPageMap(pager, 16);
        return new Mutator(pager, pageLocker, new Journal(pager, pages), pages);
    }

    public void close()
    {
    }

    private static FileChannel newFileChannel(File file)
    {
        RandomAccessFile raf;
        try
        {
            raf = new RandomAccessFile(file, "rw");
        }
        catch (FileNotFoundException e)
        {
            throw new Danger("file.not.found", e);
        }

        return raf.getChannel();
    }

    private static void checksum(Checksum checksum, ByteBuffer bytes)
    {
        checksum.reset();
        for (int i = CHECKSUM_SIZE; i < bytes.capacity(); i++)
        {
            checksum.update(bytes.get(i));
        }
        bytes.putLong(0, checksum.getValue());
    }
    
    private static SortedSet<Long> singleton(long position)
    {
        SortedSet<Long> set = new TreeSet<Long>();
        set.add(position);
        return set;
    }

    public final static class Danger
    extends RuntimeException
    {
        private static final long serialVersionUID = 20070821L;

        public Danger(String message)
        {
            super(message);
        }

        public Danger(String message, Throwable cause)
        {
            super(message, cause);
        }
    }

    public final static class Creator
    {
        private final Map<URI, Integer> mapOfStaticPageSizes;

        private int pageSize;

        private int alignment;

        private int internalJournalCount;

        public Creator()
        {
            this.mapOfStaticPageSizes = new TreeMap<URI, Integer>();
            this.pageSize = 8 * 1024;
            this.alignment = 64;
            this.internalJournalCount = 64;
        }

        public void setInternalJournalCount(int internalJournalCount)
        {
            this.internalJournalCount = internalJournalCount;
        }

        public void setPageSize(int pageSize)
        {
            this.pageSize = pageSize * 1024;
        }

        public void setAlignment(int alignment)
        {
            this.alignment = alignment;
        }

        public void addStaticPage(URI uri, int blockSize)
        {
            mapOfStaticPageSizes.put(uri, new Integer(blockSize));
        }

        /**
         * Create a new pack that writes to the specified file.
         */
        public Pack create(File file)
        {
            FileChannel fileChannel = newFileChannel(file);

            // Allocate a working buffer that will hold the header, the size
            // is rounded up to the nearest page alignment.

            int fullHeaderSize = FILE_HEADER_SIZE + internalJournalCount * POSITION_SIZE;
            if (fullHeaderSize % pageSize != 0)
            {
                fullHeaderSize += pageSize - fullHeaderSize % pageSize;
            }
            ByteBuffer header = ByteBuffer.allocateDirect(fullHeaderSize);

            // Initialize the header and write it to the file.

            int pointerPageCount = 1;

            header.putInt(pageSize);
            header.putInt(alignment);
            header.putLong(0L);
            header.putInt(internalJournalCount);
            header.putInt(pointerPageCount);

            header.clear();

            try
            {
                fileChannel.write(header, 0L);
            }
            catch (IOException e)
            {
                throw new Danger("io.write", e);
            }

            // Create a buffer of journal file positions. Initialize each page
            // position to 0. Write the journal headers to file.

            ByteBuffer journals = ByteBuffer.allocateDirect(internalJournalCount * POSITION_SIZE);

            for (int i = 0; i < internalJournalCount; i++)
            {
                journals.putLong(0L);
            }

            try
            {
                fileChannel.write(journals, FILE_HEADER_SIZE);
            }
            catch (IOException e)
            {
                throw new Danger("io.write", e);
            }

            // To create the map of static pages, we're going to allocate a
            // block from the pager. We create a local pack for this purpose.
            // This local pack will have a bogus, empty map of static pages.
            // We create a subsequent pack to return to the user.

            Map<URI, Long>mapOfStaticPages = new HashMap<URI, Long>();

            long firstPointer = FILE_HEADER_SIZE + ((internalJournalCount) * POSITION_SIZE);

            Pager pager = new Pager(file, fileChannel, pageSize, alignment, mapOfStaticPages, journals, firstPointer, pointerPageCount);

            pager.initialize();

            Pack pack = new Pack(pager);

            Mutator mutator = pack.mutate();

            for (Map.Entry<URI, Integer> entry: mapOfStaticPageSizes.entrySet())
            {
                URI uri = entry.getKey();
                int size = entry.getValue();
                long address = mutator.allocate(size);
                mapOfStaticPages.put(uri, address);
            }

            AllocOutputStream alloc = new AllocOutputStream(mutator);
            try
            {
                ObjectOutputStream out = new ObjectOutputStream(alloc);
                out.writeObject(mapOfStaticPages);
                out.close();
            }
            catch (IOException e)
            {
                throw new Danger("io.static.pages", e);
            }

            long staticPages = alloc.allocate(false);

            mutator.commit();

            pack.close();

            // Write the address of the map of static pages to the file
            // header.

            fileChannel = newFileChannel(file);

            header.clear();

            header.putInt(pageSize);
            header.putInt(alignment);
            header.putLong(staticPages);
            header.putInt(internalJournalCount);
            header.putInt(pointerPageCount);

            header.flip();

            try
            {
                fileChannel.write(header, 0L);
            }
            catch (IOException e)
            {
                throw new Danger("io.write", e);
            }

            // Return a new pack.

            return new Pack(new Pager(file, fileChannel, pageSize, alignment, mapOfStaticPages, journals, firstPointer, pointerPageCount));
        }
    }
    
    private static final class Move
    {
        private final long from;
        
        private final long to;
        
        private final Lock lock;
        
        private Move next;

        public Move(long from, long to)
        {
            assert from != to || from == 0;
            
            Lock lock = new ReentrantLock();
            lock.lock();
            
            this.from = from;
            this.to = to;
            this.lock = lock;
        }

        public Lock getLock()
        {
            return lock;
        }

        public long getFrom()
        {
            return from;
        }

        public long getTo()
        {
            return to;
        }
        
        public void setNext(Move next)
        {
            assert this.next == null;

            this.next = next;
        }
    }

    // TODO Maybe rename position. Then PageType becomes Page.
    private static final class Page
    {
        private final Pager pager;

        private PageType pageType;

        private long position;

        private Reference<ByteBuffer> bufferReference;

        public Page(Pager pager, long position)
        {
            this.pager = pager;
            this.position = position;
        }

        private ByteBuffer load(Pager pager, long position)
        {
            int pageSize = pager.getPageSize();
            int bufferSize = pageSize;
            if (position % pageSize != 0L)
            {
                bufferSize = (int) (pageSize - position % pageSize);
            }
            ByteBuffer bytes = ByteBuffer.allocateDirect(bufferSize);
            try
            {
                pager.getFileChannel().read(bytes, position);
            }
            catch (IOException e)
            {
                throw new Danger("io.page.load", e);
            }
            bytes.clear();

            return bytes;
        }

        public void setPageType(PageType pageType)
        {
            this.pageType = pageType;
        }

        public PageType getPageType()
        {
            return pageType;
        }

        public Pager getPager()
        {
            return pager;
        }

        protected Reference<ByteBuffer> getByteBufferReference()
        {
            return bufferReference;
        }

        public synchronized long getPosition()
        {
            return position;
        }

        protected synchronized void setPosition(long position)
        {
            this.position = position;
        }

        public synchronized ByteBuffer getByteBuffer()
        {
            ByteBuffer bytes = null;
            if (bufferReference == null)
            {
                bytes = load(pager, position);
                bufferReference = new WeakReference<ByteBuffer>(bytes);
            }

            bytes = (ByteBuffer) bufferReference.get();
            if (bytes == null)
            {
                bytes = load(pager, position);
                bufferReference = new WeakReference<ByteBuffer>(bytes);
            }

            return bytes;
        }
    }

    private interface PageType
    {
        public void create(Page page, DirtyPageMap pages);

        public void load(Page page);

        public boolean isSystem();
    }

    private static final class AddressPage
    implements PageType
    {
        private Page page;

        private long reservations;

        public void create(Page page, DirtyPageMap pages)
        {
            ByteBuffer bytes = page.getByteBuffer();

            int capacity = bytes.capacity() / ADDRESS_SIZE;
            for (int i = 0; i < capacity; i++)
            {
                bytes.putLong(i * ADDRESS_SIZE, 0L);
            }

            pages.put(page);

            this.page = page;
        }

        /**
         * Adjust the starting offset for addresses in the address page
         * accounting for the header and for the file header, if this is the
         * first address page in file.
         *
         * @return The start offset for iterating through the addresses.
         */
        private int getStartOffset()
        {
            int offset = 0;
            long firstPointer = page.getPager().getFirstPointer();
            if (page.getPosition() < firstPointer)
            {
                offset = (int) firstPointer / POSITION_SIZE;
            }
            return offset + (ADDRESS_PAGE_HEADER_SIZE / POSITION_SIZE);
        }
        
        public void load(Page page)
        {
            page.setPageType(this);
            this.page = page;
        }

        /**
         * TODO: This should read is wilderness.
         */
        public boolean isSystem()
        {
            return false;
        }

        /**
         * Return the page position associated with the address.
         *
         * @return The page position associated with the address.
         */
        public long dereference(long address)
        {
            synchronized (page)
            {
                int offset = (int) (address - page.getPosition());
                long position = page.getByteBuffer().getLong(offset);

                assert position != 0L; 

                return position;
            }
        }

        private ReservationPage getReservationPage(DirtyPageMap dirtyPages)
        {
            Pager pager = page.getPager();
            Page page = null;
            if (reservations == 0L)
            {
                page = pager.newSystemPage(new ReservationPage(), dirtyPages);
                reservations = page.getPosition();
            }
            else
            {
                page = pager.getPage(reservations, new ReservationPage());
            }

            return (ReservationPage) page.getPageType();
        }

        /**
         * Reserve an available address from the address page. Reserving an
         * address requires marking it as reserved on an assocated page
         * reservation page. The parallel page is necessary because we cannot
         * change the zero state of the address until the page is committed.
         * <p>
         * The reservation page is tracked with the dirty page map. It can be
         * released after the dirty page map flushes the reservation page to
         * disk.
         * 
         * @param dirtyPages The dirty page map.
         * @return An reserved address or 0 if none are available.
         */
        public long reserve(DirtyPageMap dirtyPages)
        {
            synchronized (page)
            {
                // Get the reservation page. Note that the page type holds a
                // hard reference to the page. 

                ReservationPage reserver = getReservationPage(dirtyPages);

                // Get the page buffer.
                
                ByteBuffer bytes = page.getByteBuffer();

                // Iterate the page buffer looking for a zeroed address that has
                // not been reserved, reserving it and returning it if found.
                
                for (int i = getStartOffset(); i < bytes.capacity() / ADDRESS_SIZE; i++)
                {
                    if (bytes.getLong(i) == 0L && reserver.reserve(i, dirtyPages))
                    {
                        return page.getPosition() + (i * ADDRESS_SIZE);
                    }
                }

                // Not found.
                
                return 0L;
            }
        }

        public long allocate()
        {
            synchronized (page)
            {
                ByteBuffer bytes = page.getByteBuffer();
                int addresses = bytes.capacity() / ADDRESS_SIZE;
                for (int i = 0; i < addresses; i++)
                {
                    if (bytes.getLong(i * ADDRESS_SIZE) > 0L)
                    {
                        bytes.putLong(i * ADDRESS_SIZE, -1L);
                        return page.getPosition() + i * ADDRESS_SIZE;
                    }
                }
            }
            throw new IllegalStateException();
        }

        public void set(long address, long position)
        {
            synchronized (page)
            {
                ByteBuffer bytes = page.getByteBuffer();
                bytes.putLong((int) (address - page.getPosition()), position);
            }
        }

        public long newAddress(long firstPointer)
        {
            return 0;
        }
    }

    private static class RelocatablePage
    implements PageType
    {
        private Page page;

        private Allocator allocator;

        public void create(Page page, DirtyPageMap pages)
        {
            this.page = page;
            page.setPageType(this);
        }

        public void load(Page page)
        {
            page.setPageType(this);
        }

        protected Page getPage()
        {
            return page;
        }

        public Allocator getAllocator()
        {
            synchronized (page)
            {
                return allocator;
            }
        }

        public void clearAllocator()
        {
            synchronized (page)
            {
                this.allocator = NULL_ALLOCATOR;
                page.notifyAll();
            }
        }

        public boolean isSystem()
        {
            return false;
        }

        protected void initialize(ByteBuffer bytes)
        {
        }
    }

    private static final class DataPage
    extends RelocatablePage
    {
        private int remaining;

        private int count;

        private boolean system;
        
        public void create(Page page, DirtyPageMap pages)
        {
            super.create(page, pages);
            
            this.count = 0;
            this.remaining = page.getPager().getPageSize() - DATA_PAGE_HEADER_SIZE; 
        }

        public void load(Page page)
        {
            ByteBuffer bytes = page.getByteBuffer();
            this.count = bytes.getInt();
            this.remaining = getRemaining(count, bytes);
        }

        private static int getRemaining(int count, ByteBuffer bytes)
        {
            for (int i = 0; i < count; i++)
            {
                bytes.getLong();
                int size = bytes.getInt();
                int advance = Math.abs(size) > bytes.remaining() ? bytes.remaining() : Math.abs(size);
                bytes.position(bytes.position() + advance);
            }
            return bytes.remaining();
        }

        public boolean isSystem()
        {
            return system;
        }

        public int getCount()
        {
            synchronized (getPage())
            {
                return count;
            }
        }

        public int getRemaining()
        {
            synchronized (getPage())
            {
                return remaining;
            }
        }

        public void reset(short type)
        {
            synchronized (getPage())
            {
                this.count = 0;
                this.remaining = getRemaining(count, getPage().getByteBuffer());
            }
        }

        private int getSize(ByteBuffer bytes)
        {
            int size = bytes.getInt();
            if (Math.abs(size) > bytes.remaining())
            {
                throw new IllegalStateException();
            }
            return size;
        }

        private boolean seek(ByteBuffer bytes, int offset)
        {
            int block = 0;
            while (bytes.position() != offset && block < count)
            {
                int size = getSize(bytes);
                if (size > 0)
                {
                    block++;
                }
                bytes.position(bytes.position() + Math.abs(size));
            }
            return block < count;
        }

        private boolean seek(ByteBuffer bytes, long address)
        {
            int block = 0;
            while (block < count)
            {
                int size = getSize(bytes);
                if (size > 0)
                {
                    block++;
                }
                if (bytes.getLong(bytes.position()) == address)
                {
                    bytes.position(bytes.position() - COUNT_SIZE);
                    return true;
                }
                bytes.position(bytes.position() + Math.abs(size));
            }
            return false;
        }

        public ByteBuffer read(long position, long address)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getBlockRange(getPage().getByteBuffer());
                int offset = getOffset(position);

                if (seek(bytes, offset))
                {
                    int size = getSize(bytes);
                    bytes.limit(bytes.position() + size);
                    if (bytes.getLong() != address)
                    {
                        throw new IllegalStateException();
                    }
                    return bytes.slice();
                }
            }
            throw new ArrayIndexOutOfBoundsException();
        }

        public void vacuum(Journal journal, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                journal.markVacuum(getPage().getPosition());
                ByteBuffer bytes = getBlockRange(getPage().getByteBuffer());
                int block = 0;
                while (block != count)
                {
                    int size = getSize(bytes);
                    if (size < 0)
                    {
                        bytes.position(bytes.position() + Math.abs(size));
                    }
                    else
                    {
                        block++;
                        long address = bytes.getLong(bytes.position());
                        if (size > remaining)
                        {
                            throw new IllegalStateException();
                        }
                        ByteBuffer fromBuffer = ByteBuffer.allocateDirect(Math.abs(size));
                        fromBuffer.put(bytes);

                        long fromPosition = journal.allocate(size, address);
                        journal.write(fromPosition, address, fromBuffer);

                        journal.write(new Shift(address, fromPosition));
                    }
                }
            }
        }

        public long allocate(int length, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getBlockRange(getPage().getByteBuffer());
                long position = 0L;
                int block = 0;
                while (block != count && position == 0L)
                {
                    int size = getSize(bytes);
                    if (size > 0)
                    {
                        block++;
                    }
                    bytes.position(bytes.position() + Math.abs(size));
                }
                return getPage().getPosition() + bytes.position();
            }
        }

        public long allocate(long address, int length, DirtyPageMap pages)
        {
            long position = 0L;
            synchronized (getPage())
            {
                ByteBuffer bytes = getBlockRange(getPage().getByteBuffer());
                int block = 0;
                while (block != count && position == 0L)
                {
                    int offset = bytes.position();
                    int size = getSize(bytes);
                    if (size > 0 && bytes.getLong(bytes.position()) == address)
                    {
                        position = getPage().getPosition() + offset;
                    }
                    else
                    {
                        bytes.position(bytes.position() + Math.abs(size));
                    }
                }

                if (position == 0L)
                {
                    position = getPage().getPosition() + bytes.position();

                    bytes.putInt(length);
                    bytes.putLong(address);

                    count++;

                    bytes.clear();
                    bytes.getLong();
                    bytes.putInt(count);
                }
            }

            pages.put(getPage());

            return position;
        }

        public boolean write(Allocator allocator, long address, ByteBuffer data, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                if (getAllocator() == allocator)
                {
                    allocator.rewrite(address, data);
                    ByteBuffer bytes = getBlockRange(getPage().getByteBuffer());
                    if (seek(bytes, address))
                    {
                        int size = getSize(bytes);
                        bytes.putLong(address);
                        bytes.limit(size - POSITION_SIZE);
                        bytes.put(data);
                        pages.put(getPage());
                        return true;
                    }
                }
                return false;
            }
        }

        public void write(long position, long address, ByteBuffer data, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getPage().getByteBuffer();
                bytes.clear();
                bytes.position((int) (position - getPage().getPosition()));
                // FIXME Dies here because size is zero. Obviously not being
                // written correctly.
                int size = bytes.getInt();
                if (address != bytes.getLong())
                {
                    throw new IllegalStateException();
                }
                bytes.limit(bytes.position() + (size - BLOCK_HEADER_SIZE));
                bytes.put(data);
                pages.put(getPage());
            }
        }

        private ByteBuffer getBlockRange(ByteBuffer bytes)
        {
            bytes.position(DATA_PAGE_HEADER_SIZE);
            bytes.limit(bytes.capacity());
            return bytes;
        }

        private int getOffset(long position)
        {
            return (int) (position - getPage().getPosition());
        }

        public boolean free(Allocator allocator, long address, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                if (getAllocator() == allocator)
                {
                    allocator.unwrite(address);
                    ByteBuffer bytes = getBlockRange(getPage().getByteBuffer());
                    if (seek(bytes, address))
                    {
                        int offset = bytes.position();

                        int size = getSize(bytes);
                        if (size > 0)
                        {
                            size = -size;
                        }
                        bytes.putInt(offset, size);

                        pages.put(getPage());
                    }
                    return true;
                }
                return false;
            }
        }

        public void relocate(Page to, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                getPage().setPosition(to.getPosition());
                ByteBuffer bytes = to.getByteBuffer();
                for (int i = 0; i < count; i++)
                {
                    int offset = bytes.position();
                    int length = bytes.getInt();
                    long address = bytes.getLong();

                    Page addressPage = getPage().getPager().getPage(address, new AddressPage());

                    AddressPage addressType = (AddressPage) addressPage.getPageType();
                    addressType.set(address, getPage().getPosition() + offset);

                    pages.put(addressPage);

                    bytes.position(bytes.position() + length - POSITION_SIZE);
                }
            }
        }
    }

    private final static class JournalPage
    extends RelocatablePage
    {
        private ByteBuffer bytes; // TODO Necessary?

        private int offset;

        public void create(Page page, DirtyPageMap pages)
        {
            super.create(page, pages);

            bytes = getPage().getByteBuffer();
            
            bytes.clear();
            bytes.getLong();
            bytes.putInt(-1);

            offset = bytes.position();

            getPage().setPageType(this);
        }

        public void load(Page page)
        {
            super.load(page);

            bytes.putLong(0L);
            bytes.putInt(-1);
        }

        public boolean isSystem()
        {
            return true;
        }

        public boolean write(Operation operation, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getPage().getByteBuffer();
                if (operation.length() + NEXT_PAGE_SIZE < bytes.capacity() - offset)
                {
                    bytes.position(offset);
                    operation.write(bytes);
                    pages.put(getPage());
                    return true;
                }
                return false;
            }
        }

        public void next(long position)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getPage().getByteBuffer();
                bytes.position(offset);
                new NextOperation(position).write(bytes);
            }
        }

        public long getJournalPosition()
        {
            synchronized (getPage())
            {
                return getPage().getPosition() + offset;
            }
        }

        public void seek(long position)
        {

        }

        public Operation next()
        {
            return null;
        }

        public void putBack(Shift shift, DirtyPageMap pages)
        {
        }
    }

    private static final class ReservationPage
    extends RelocatablePage
    {
        private int addressCount;

        public void create(Page page, DirtyPageMap pages)
        {
            super.create(page, pages);

            ByteBuffer bytes = page.getByteBuffer();

            bytes.clear();

            bytes.putLong(0L);
            bytes.putInt(-1);
            bytes.putInt(0);

            pages.put(page);
        }

        public void load(Page page)
        {
            super.load(page);

            ByteBuffer bytes = page.getByteBuffer();
            this.addressCount = Math.abs(bytes.getInt(CHECKSUM_SIZE + COUNT_SIZE));
        }

        protected void initialize(ByteBuffer bytes)
        {
            bytes.putLong(0L);
            bytes.putInt(-1);
            bytes.putInt(0);
        }

        public boolean isSystem()
        {
            return true;
        }

        private int search(ByteBuffer bytes, int offset)
        {
            synchronized (getPage())
            {
                int low = 0;
                int high = addressCount;
                int mid = 0;
                int cur = 0;
                while (low <= high)
                {
                    mid = (low + high) / 2;
                    cur = bytes.getInt(RESERVATION_PAGE_HEADER_SIZE + (COUNT_SIZE * mid));
                    if (cur > offset)
                    {
                        high = mid - 1;
                    }
                    else if (cur < offset)
                    {
                        low = mid + 1;
                    }
                    else
                    {
                        return mid;
                    }
                }
                if (cur > offset)
                {
                    return -(mid - 1);
                }
                return -(mid + 1);
            }
        }

        public boolean reserve(int offset, DirtyPageMap pages)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getPage().getByteBuffer();
                int index = addressCount == 0 ? 0 : search(bytes, offset);
                if (index < 0 || addressCount == 0)
                {
                    index = -index;
                    for (int i = addressCount; i > index; i--)
                    {
                        bytes.putInt((i + 5) * COUNT_SIZE, bytes.getInt((i + 4) * COUNT_SIZE));
                    }
                    bytes.putInt(index, offset);
                    addressCount++;
                    bytes.putInt(CHECKSUM_SIZE, addressCount);
                    pages.put(getPage());
                    return true;
                }
                return false;
            }
        }

        public void remove(int offset)
        {
            synchronized (getPage())
            {
                ByteBuffer bytes = getPage().getByteBuffer();
                int index = search(bytes, offset);
                if (index > 0)
                {
                    for (int i = index; i < addressCount - 1; i++)
                    {
                        bytes.putInt((i + 4) * COUNT_SIZE, bytes.getInt((i + 5) * COUNT_SIZE));
                    }
                    addressCount--;
                    bytes.putInt(CHECKSUM_SIZE + COUNT_SIZE, addressCount);
                }
            }
        }
    }

    private static final class PageReference
    extends WeakReference<Page>
    {
        private final Long position;

        public PageReference(Page page, ReferenceQueue<Page> queue)
        {
            super(page, queue);
            this.position = new Long(page.getPosition());
        }

        public Long getPosition()
        {
            return position;
        }
    }

    private final static class Size
    {
        private final Page page;

        private final int remaining;

        public Size(Page page)
        {
            this.page = page;
            this.remaining = ((DataPage) page.getPageType()).getRemaining();
        }

        public Size(Page page, int size)
        {
            this.page = page;
            this.remaining = size;
        }

        public Page getPage()
        {
            return page;
        }
        
        /**
         * The size of the bytes on the page. This may be different than block size 
         * less remaining, since a temporary allocated block may be deleted before commit.
         *  
         * @return
         */
        public int getSize()
        {
            return 0;
        }

        public int getRemaining()
        {
            return remaining;
        }
    }
    
    private final static class PageLocker
    {
        private int waitingReaders;
        
        private final Object shared;
        
        private int waitingWriters;
        
        private final Object exclusive;
        
        private final List<Map<Long, int[]>> listOfMapsOfPages;
        
        /**
         * Create a new page locker with the specified number of parallel hash
         * tables. 
         * 
         * @param parallels
         */
        public PageLocker(int parallels)
        {
            List<Map<Long, int[]>> listOfMapsOfPages = new ArrayList<Map<Long, int[]>>(parallels);
            for (int i = 0; i < parallels; i++)
            {
                listOfMapsOfPages.add(new HashMap<Long, int[]>());
            }
            this.listOfMapsOfPages = listOfMapsOfPages;
            this.shared = new Object();
            this.exclusive = new Object();
        }
        
        private int hashIndex(Long position)
        {
            return (int) (position.longValue() % listOfMapsOfPages.size());
        }
 
        private boolean acquireShared(long position, boolean waiting)
        {
            int[] value;
            Map<Long, int[]> mapOfPages = listOfMapsOfPages.get(hashIndex(position));
            synchronized (mapOfPages)
            {
                value = mapOfPages.get(position);

                assert value == null || value[0] != 0;

                if (value == null)
                {
                    value = new int[] { 1 };
                    mapOfPages.put(position, value);
                }
                else if (value[0] > 0)
                {
                    value[0]++;
                }
            }
            boolean pass = value[0] > 0;

            if (waiting && pass)
            {
                synchronized (this)
                {
                    waitingReaders--;
                }
            }
            else if (!waiting && !pass)
            {
                synchronized (this)
                {
                    waitingReaders++;
                }
            }

            return pass;
        }
        
        private void release(long position, int increment)
        {
            Map<Long, int[]> mapOfPages = listOfMapsOfPages.get(hashIndex(position));
            synchronized (mapOfPages)
            {
                int[] value = mapOfPages.get(position);
                
                assert value == null || value[0] != 0;
                
                value[0] += increment;
                
                if (value[0] == 0)
                {
                    mapOfPages.remove(position);
                }
            }
        }

        public void acquireShared(SortedSet<Long> setOfPages)
        {
            try
            {
                boolean waiting = false;
                Iterator<Long> pages = setOfPages.iterator();
                while (pages.hasNext())
                {
                    long position = pages.next();
                    for (;;)
                    {
                        if (!acquireShared(position, waiting))
                        {
                            waiting = true;
                            shared.wait();
                        }
                        else
                        {
                            waiting = false;
                            break;
                        }
                    }
                }
            }
            catch (InterruptedException e)
            {
            }
        }
        
        public void releaseShared(SortedSet<Long> setOfPages)
        {
            Iterator<Long> pages = setOfPages.iterator();
            while (pages.hasNext())
            {
                long position = pages.next();
                release(position, -1);
            }
            
            signal();
        }
        
        private boolean acquireExclusive(Long position, boolean waiting)
        {
            int[] value;
            Map<Long, int[]> mapOfPages = listOfMapsOfPages.get(hashIndex(position));
            synchronized (mapOfPages)
            {
                value = mapOfPages.get(position);
                
                assert value == null || value[0] > 0;
                
                if (value == null)
                {
                    value = new int[] { -1 };
                    mapOfPages.put(position, value);
                }
            }
            boolean pass = value != null;
            
            if (waiting && pass)
            {
                synchronized (this)
                {
                    waitingWriters--;
                }
            }
            else if (!waiting && !pass)
            {
                synchronized (this)
                {
                    waitingWriters--;
                }
            }
            
            return pass;
        }

        public void acquireExclusive(SortedSet<Long> setOfPages)
        {
            try
            {
                boolean waiting = false;
                Iterator<Long> pages = setOfPages.iterator();
                while (pages.hasNext())
                {
                    long position = pages.next();
                    for (;;)
                    {
                        if (!acquireExclusive(position, waiting))
                        {
                            waiting = true;
                            shared.wait();
                        }
                        else
                        {
                            waiting = false;
                            break;
                        }
                    }
                }
            }
            catch (InterruptedException e)
            {
            }
        }
        
        public void releaseExclusive(SortedSet<Long> setOfPages)
        {
            Iterator<Long> pages = setOfPages.iterator();
            while (pages.hasNext())
            {
                long position = pages.next();
                release(position, 1);
            }
        }
        
        private synchronized void signal()
        {
            if (waitingReaders > 1)
            {
                exclusive.notifyAll();
            }
        }
    }

    private final static class BySizeTable
    {
        private final int alignment;

        private final List<LinkedList<Size>> listOfListsOfSizes;

        public BySizeTable(int pageSize, int alignment)
        {
            assert pageSize % alignment == 0;

            ArrayList<LinkedList<Size>> listOfListsOfSizes = new ArrayList<LinkedList<Size>>(pageSize / alignment);

            for (int i = 0; i < pageSize / alignment; i++)
            {
                listOfListsOfSizes.add(new LinkedList<Size>());
            }

            this.alignment = alignment;
            this.listOfListsOfSizes = listOfListsOfSizes;
        }
        
        public int getSize()
        {
            int size = 0;
            for (List<Size> listOfSizes : listOfListsOfSizes)
            {
                size += listOfSizes.size();
            }
            return size;
        }

        public void add(Page page)
        {
            add(new Size(page, ((DataPage) page.getPageType()).getRemaining()));
        }

        public synchronized void add(Size size)
        {
            // Maybe don't round down if exact.
            int aligned = ((size.getRemaining() | alignment - 1) + 1) - alignment;
            if (aligned != 0)
            {
                listOfListsOfSizes.get(aligned / alignment).addFirst(size);
            }
        }

        /**
         * Return the page with the least amount of space remaining that will
         * fit the full block size. The block specified block size must
         * includes the block header.
         * <p>
         * The method will ascend the table looking at the slots for each
         * remaining size going form smallest to largest and returning the
         * first to fit the block, or null if no page can fit the block.
         * 
         * @param blockSize
         *            The block size including the block header.
         * @return A size object containing the a page that will fit the block
         *         or null if none exists.
         */
        public synchronized Size bestFit(int blockSize)
        {
            Size bestFit = null;
            int aligned = ((blockSize | alignment - 1) + 1); // Round up.
            if (aligned != 0)
            {
                for (int i = aligned / alignment; bestFit == null && i < listOfListsOfSizes.size(); i++)
                {
                    if (!listOfListsOfSizes.get(i).isEmpty())
                    {
                        bestFit = listOfListsOfSizes.get(i).removeFirst();
                    }
                }
            }
            return bestFit;
        }

        public synchronized void _remove(Page page)
        {
            // FIXME Does not work. We need to know the size. Where is this used?
            DataPage dataPage = (DataPage) page.getPageType();

            int aligned = ((dataPage.getRemaining() | alignment - 1) + 1) - alignment;

            LinkedList<Size> listOfSizes = listOfListsOfSizes.get(aligned / alignment);

            Iterator<Size> pages = listOfSizes.iterator();
            while (pages.hasNext())
            {
                Size candidate = (Size) pages.next();
                if (candidate.getPage().getPosition() == page.getPosition())
                {
                    pages.remove();
                    return;
                }
            }

            throw new RuntimeException("Unmatched address in by size map.");
        }
        
        public synchronized void join(BySizeTable pagesBySize, Map<Long, Size> mapOfSizes)
        {
            for (List<Size> listOfSizes: pagesBySize.listOfListsOfSizes)
            {
                for(Size size: listOfSizes)
                {
                    Size found = bestFit(size.getSize());
                    if (found != null)
                    {
                        mapOfSizes.put(size.getPage().getPosition(), found);
                    }
                }
            }
        }
    }
    
    private final static class Reverse<T extends Comparable<T>> implements Comparator<T>
    {
        public int compare(T left, T right)
        {
            return right.compareTo(left);
        }
    }

    private final static class Pager
    {
        private final Checksum checksum;

        private final FileChannel fileChannel;
        
        private final int pageSize;

        private final Map<Long, PageReference> mapOfPagesByPosition;
        
        private final ReadWriteLock goalPostLock;

        // FIXME Outgoing.
        private final Set<Writer> setOfWriters;

        private final ReferenceQueue<Page> queue;

        public final File file;

        public final Map<URI, Long> mapOfStaticPages;

        private final int alignment;

        public final BySizeTable pagesBySize;

        private final Map<Long, Page> mapOfPointerPages;

        private final SortedSet<Long> setOfFreeUserPages;

        /**
         * A sorted set of of free interim pages sorted in descending order so
         * that we can quickly obtain the last free interim page within interim
         * page space.
         */
        private final SortedSet<Long> setOfFreeInterimPages;

        private final ByteBuffer journalBuffer;

        private int pointerPageCount;

        private final long firstPointer;

        private final ByteBuffer pointerPageCountBytes;

        private long firstSystemPage;
        
        private Move headOfMoves;

        public Pager(File file, FileChannel fileChannel, int pageSize, int alignment, Map<URI, Long> mapOfStaticPages, ByteBuffer journalBuffer, long firstPointer, int pointerPageCount)
        {
            this.file = file;
            this.fileChannel = fileChannel;
            this.alignment = alignment;
            this.pageSize = pageSize;
            this.firstPointer = firstPointer;
            this.pointerPageCount = pointerPageCount;
            this.checksum = new Adler32();
            this.mapOfPagesByPosition = new HashMap<Long, PageReference>();
            this.setOfWriters = new HashSet<Writer>();
            this.journalBuffer = journalBuffer;
            this.mapOfPointerPages = new HashMap<Long, Page>();
            this.pagesBySize = new BySizeTable(pageSize, alignment);
            this.mapOfStaticPages = mapOfStaticPages;
            this.pointerPageCountBytes = ByteBuffer.allocateDirect(COUNT_SIZE);
            this.setOfFreeUserPages = new TreeSet<Long>();
            this.setOfFreeInterimPages = new TreeSet<Long>(new Reverse<Long>());
            this.queue = new ReferenceQueue<Page>();
            this.headOfMoves = new Move(0, 0);
            this.headOfMoves.getLock().unlock();
            this.goalPostLock = new ReentrantReadWriteLock();
        }
        
        public long getFirstPointer()
        {
            return firstPointer;
        }
        
        public ReadWriteLock getGoalPostLock()
        {
            return goalPostLock;
        }

        /**
         * Initialize the 
         */
        public void initialize()
        {
            long wilderness;
            try
            {
                wilderness = fileChannel.size();
            }
            catch (IOException e)
            {
                throw new Danger("io.size", e);
            }

            if (wilderness % pageSize != 0)
            {
                throw new IllegalStateException();
            }

            long firstPointerPage = firstPointer;
            firstPointerPage -= firstPointerPage % pageSize;
            long firstUserPage = firstPointerPage + pointerPageCount * pageSize;

            ByteBuffer bytes = ByteBuffer.allocate(pageSize);
            long position = wilderness - pageSize;
            if (position == 0L)
            {
                firstSystemPage = position + pageSize;
            }
            
            while (firstPointerPage < firstUserPage)
            {
                try
                {
                    fileChannel.read(bytes);
                }
                catch (IOException e)
                {
                    throw new Danger("io.read", e);
                }
                bytes.flip();
                int offset = 0;
                if (firstPointerPage < firstPointer)
                {
                    offset = (int) firstPointer / POSITION_SIZE;
                }
                while (offset < bytes.capacity() / POSITION_SIZE)
                {
                    if (bytes.getLong(offset) == 0L)
                    {   
                        Page page = getPage(firstPointerPage, new AddressPage());
                        mapOfPointerPages.put(new Long(firstPointerPage), page);
                        break;
                    }
                }
                firstPointerPage += pageSize;
            }
        }

        public int getAlignment()
        {
            return alignment;
        }

        public FileChannel getFileChannel()
        {
            return fileChannel;
        }

        public int getPageSize()
        {
            return pageSize;
        }

        public void addWriter(Writer writer)
        {
            synchronized (setOfWriters)
            {
                setOfWriters.add(writer);
            }
        }

        public void removeWriter(Writer writer)
        {
            synchronized (setOfWriters)
            {
                setOfWriters.remove(writer);
            }
        }

        public void newUserDataPages(int needed, Set<Long> setOfPages)
        {
            synchronized (setOfFreeUserPages)
            {
                while (setOfFreeUserPages.size() != 0 && needed != 0)
                {
                    Iterator<Long> freeUserPages = setOfFreeUserPages.iterator();
                    setOfPages.add(freeUserPages.next());
                    freeUserPages.remove();
                }
            }
        }

        public synchronized void collect()
        {
            PageReference pageReference = null;
            while ((pageReference = (PageReference) queue.poll()) != null)
            {
                mapOfPagesByPosition.remove(pageReference.getPosition());
            }
        }

        public synchronized long reserve(Page page, DirtyPageMap pages)
        {
            long address = ((AddressPage) page.getPageType()).reserve(pages);
            if (address == 0L)
            {
                mapOfPointerPages.remove(new Long(page.getPosition()));
            }
            return address;
        }

        public synchronized Page getAddressPage(long position, DirtyPageMap pages)
        {
            Page addressPage = null;
            if (mapOfPointerPages.size() == 0)
            {
                Journal journal = new Journal(this, new DirtyPageMap(this, 16));
                long firstDataPage = 0L;
                Page fromDataPage = getPage(firstDataPage, new DataPage());
                Page toDataPage = newDataPage(pages);
                Allocator allocator = ((DataPage) fromDataPage.getPageType()).getAllocator();
                journal.relocate(allocator, fromDataPage, toDataPage);
            }
            if (position != 0L)
            {
                addressPage = getPage(position, new AddressPage());
            }
            if (addressPage == null)
            {
                addressPage = (Page) mapOfPointerPages.values().iterator().next();
            }
            return addressPage;
        }

        private long fromWilderness()
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);

            bytes.getLong(); // Checksum.
            bytes.putInt(-1); // Is system page.

            bytes.clear();

            checksum(checksum, bytes);

            long position;

            synchronized (fileChannel)
            {
                try
                {
                    position = fileChannel.size();
                }
                catch (IOException e)
                {
                    throw new Danger("io.size", e);
                }

                try
                {
                    fileChannel.write(bytes, position);
                }
                catch (IOException e)
                {
                    throw new Danger("io.write", e);
                }

                try
                {
                    if (fileChannel.size() % 1024 != 0)
                    {
                        throw new Danger("io.position");
                    }
                }
                catch (IOException e)
                {
                    throw new Danger("io.size", e);
                }
            }

            return position;
        }

        public void freeSystemPage(long position)
        {
            synchronized (setOfFreeInterimPages)
            {
                if (position >= firstSystemPage)
                {
                    setOfFreeInterimPages.add(new Long(position));
                }
            }
        }

        public Page newDataPage(DirtyPageMap pages)
        {
            DataPage dataPage = new DataPage();
            Long address = null;
            synchronized (setOfFreeUserPages)
            {
                Iterator<Long> userPages = setOfFreeUserPages.iterator();
                if (userPages.hasNext())
                {
                    address = userPages.next();
                    userPages.remove();
                }
            }
            if (address != null)
            {
                return getPage(address.longValue(), dataPage);
            }
            Page page = null;
            synchronized (setOfFreeInterimPages)
            {
                Iterator<Long> systemPages = setOfFreeInterimPages.iterator();
                if (systemPages.hasNext())
                {
                    address = systemPages.next();
                    if (address.longValue() == firstSystemPage)
                    {
                        synchronized (mapOfPagesByPosition)
                        {
                            page = getPageByPosition(firstSystemPage);
                            if (page == null)
                            {
                                page = new Page(this, firstSystemPage);
                                addPageByPosition(page);
                            }
                            dataPage.create(page, pages);
                        }
                    }
                }
                if (page == null)
                {
                    synchronized (mapOfPagesByPosition)
                    {
                        page = getPageByPosition(firstSystemPage);
                        assert page != null;
                    }
                    // Move.
                }
                firstSystemPage += getPageSize();
            }
            return page;
        }

        private Page getPageByPosition(long position)
        {
            Page page = null;
            Long boxPosition = new Long(position);
            PageReference chunkReference = (PageReference) mapOfPagesByPosition.get(boxPosition);
            if (chunkReference != null)
            {
                page = (Page) chunkReference.get();
            }
            return page;
        }

        private void removePageByPosition(long position)
        {
            PageReference existing = (PageReference) mapOfPagesByPosition.get(new Long(position));
            if (existing != null)
            {
                existing.enqueue();
                collect();
            }
        }

        private void addPageByPosition(Page page)
        {
            PageReference intended = new PageReference(page, queue);
            PageReference existing = (PageReference) mapOfPagesByPosition.get(intended.getPosition());
            if (existing != null)
            {
                existing.enqueue();
                collect();
            }
            mapOfPagesByPosition.put(intended.getPosition(), intended);
        }

        public Page getPage(long position, PageType pageType)
        {
            synchronized (mapOfPagesByPosition)
            {
                position = (long) Math.floor(position - (position % pageSize));
                Page page = getPageByPosition(position);
                if (page == null)
                {
                    page = new Page(this, position);
                    pageType.load(page);
                    addPageByPosition(page);
                }
                else if(page.getPageType().getClass() != pageType.getClass())
                {
                    pageType.load(page);
                }
                return page;
            }
        }

        // FIXME Too many flavors of newSystemPage.
        public Page newSystemPage(PageType pageType, DirtyPageMap pages)
        {
            // We pull from the end of the interim space to take pressure of of
            // the durable pages, which are more than likely multiply in number
            // and move interim pages out of the way. We could change the order
            // of the interim page set, so that we choose free interim pages
            // from the front of the interim page space, if we want to rewind
            // the iterim page space and shrink the file more frequently.

            long position = 0L;

            synchronized (setOfFreeInterimPages)
            {
                if (setOfFreeInterimPages.size() > 0)
                {
                    Long next = (Long) setOfFreeInterimPages.iterator().next();
                    position = next.longValue();
                }
            }

            // If we do not have a free interim page available, we will obtain
            // create one out of the wilderness.

            if (position == 0L)
            {
                position = fromWilderness();
            }

            // FIXME A concurrency question, what if this interim page is in the
            // midst of a move? If we removed it from the set of interim pages,
            // then a moving thread might be attempting to create a relocatable
            // page and moving the page. We need to lock it!?

            Page page = new Page(this, position);

            pageType.create(page, pages);

            synchronized (mapOfPagesByPosition)
            {
                addPageByPosition(page);
            }

            return page;
        }

        public long getPosition(long address)
        {
            return (long) Math.floor(address / pageSize);
        }
    }

    public final static class DirtyPageMap
    {
        private final Pager pager;

        private final Map<Long, Page> mapOfPages;

        private final Map<Long, ByteBuffer> mapOfByteBuffers;

        private final int capacity;

        public DirtyPageMap(Pager pager, int capacity)
        {
            this.pager = pager;
            this.mapOfPages = new HashMap<Long, Page>();
            this.mapOfByteBuffers = new HashMap<Long, ByteBuffer>();
            this.capacity = capacity;
        }

        public void put(Page page)
        {
            mapOfPages.put(page.getPosition(), page);
            mapOfByteBuffers.put(page.getPosition(), page.getByteBuffer());
            if (mapOfPages.size() > capacity)
            {
                flush();
            }
        }

        public void flush()
        {
            FileChannel fileChannel = pager.getFileChannel();
            for (Page page: mapOfPages.values())
            {
                synchronized (page)
                {
                    ByteBuffer bytes = page.getByteBuffer();
                    bytes.clear();
                    try
                    {
                        fileChannel.write(bytes, page.getPosition());
                    }
                    catch (IOException e)
                    {
                        throw new Danger("io.write", e);
                    }
                }
            }
            mapOfPages.clear();
            mapOfByteBuffers.clear();
        }

        public void commit(ByteBuffer journal, long position)
        {
            flush();
            FileChannel fileChannel = pager.getFileChannel();
            try
            {
                fileChannel.write(journal, position);
            }
            catch (IOException e)
            {
                throw new Danger("io.write", e);
            }
            try
            {
                fileChannel.force(true);
            }
            catch (IOException e)
            {
                throw new Danger("io.force", e);
            }
        }
    }

    private final static class Pointer
    {
        private final ByteBuffer slice;

        private final long position;

        private final Object mutex;

        public Pointer(ByteBuffer slice, long position, Object mutex)
        {
            this.slice = slice;
            this.position = position;
            this.mutex = mutex;
        }

        public ByteBuffer getByteBuffer()
        {
            return slice;
        }

        public long getPosition()
        {
            return position;
        }

        public Object getMutex()
        {
            return mutex;
        }
    }

    private final static class PositionBuffer
    {
        private final ByteBuffer bytes;

        private final long position;

        public PositionBuffer(ByteBuffer bytes, long position)
        {
            this.bytes = bytes;
            this.position = position;
        }

        public synchronized Pointer getPosition(long pointee)
        {
            Pointer pointer = null;
            for (;;)
            {
                for (int i = 0; i < bytes.remaining() / POSITION_SIZE; i++)
                {
                    if (bytes.getLong(i * POSITION_SIZE) == 0L)
                    {
                        bytes.position(i * POSITION_SIZE);
                        bytes.limit(bytes.position() + POSITION_SIZE);
                        pointer = new Pointer(bytes.slice(), position + i * POSITION_SIZE, this);
                    }
                }
                if (pointer == null)
                {
                    try
                    {
                        wait();
                    }
                    catch (InterruptedException e)
                    {
                        throw new Danger("interrupted", e);
                    }
                }
                else
                {
                    break;
                }
            }
            return pointer;
        }

        public synchronized void addPosition(long pointee)
        {
            boolean found = false;
            for (int i = 0; !found && i < bytes.remaining() / POSITION_SIZE; i++)
            {
                if (bytes.getLong(i * POSITION_SIZE) == pointee)
                {
                    bytes.putLong(i * POSITION_SIZE, 0L);
                    notify();
                    found = true;
                }
            }
        }
    }

    private abstract static class Operation
    {
        public void commit(Pager pager, Journal journal, DirtyPageMap pages)
        {
        }

        public Page getJournalPage(Pager pager, Page page)
        {
            return page;
        }

        public boolean write(Pager pager, long destination, ByteBuffer data, DirtyPageMap pages)
        {
            return false;
        }

        public boolean unwrite(Page journalPage, long destination)
        {
            return false;
        }

        public abstract int length();

        public abstract void write(ByteBuffer bytes);

        public abstract void read(ByteBuffer bytes);
    }

    private final static class Allocate
    extends Operation
    {
        private long address;

        private long position;

        private long page;

        private int length;

        public Allocate(long address, long page, long position, int length)
        {
            this.address = address;
            this.page = page;
            this.position = position; // TODO ???
            this.length = length;
        }

        public void commit(Pager pager, Journal journal, DirtyPageMap pages)
        {
            Page p = pager.getPage(page, new DataPage());
            ((DataPage) p.getPageType()).allocate(address, length, pages);
        }

        public int length()
        {
            return ADDRESS_SIZE * 3 + COUNT_SIZE;
        }

        public void write(ByteBuffer bytes)
        {
            bytes.putShort(ALLOCATE);
            bytes.putLong(address);
            bytes.putLong(page);
            bytes.putLong(position);
            bytes.putInt(length);
        }

        public void read(ByteBuffer bytes)
        {
            address = bytes.getLong();
            page = bytes.getLong();
            position = bytes.getLong();
            length = bytes.getInt();
        }
    }

    private final static class Shift
    extends Operation
    {
        private long address;

        private long source;

        public Shift(long address, long source)
        {
            this.address = address;
            this.source = source;
        }

        public long getAddress()
        {
            return address;
        }

        public long getSource()
        {
            return source;
        }

        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 2;
        }

        public void write(ByteBuffer bytes)
        {
            bytes.putShort(SHIFT);
            bytes.putLong(address);
            bytes.putLong(source);
        }

        public void read(ByteBuffer bytes)
        {
            address = bytes.getLong();
            source = bytes.getLong();
        }

        public boolean write(Pager pager, long destination, ByteBuffer data, DirtyPageMap pages)
        {
            if (address == destination)
            {
                Page page = pager.getPage(source, new DataPage());
                ((DataPage) page.getPageType()).write(source, address, data, pages);
                return true;
            }
            return false;
        }

        public boolean unwrite(JournalPage journalPage, long destination, DirtyPageMap pages)
        {
            if (address == destination)
            {
                source = 0L;
                journalPage.putBack(this, pages);
                return true;
            }
            return false;
        }
    }

    private final static class Vacuum
    extends Operation
    {
        public void commit(Pager pager, Journal journal, DirtyPageMap pages)
        {
        }

        public int length()
        {
            return FLAG_SIZE;
        }

        public void write(ByteBuffer bytes)
        {
            bytes.putShort(VACUUM);
        }

        public void read(ByteBuffer bytes)
        {
        }
    }

    private final static class Relocate
    extends Operation
    {
        private long from;

        private long to;

        public Relocate(long from, long to)
        {
            this.from = from;
            this.to = to;
        }

        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 2;
        }

        public void write(ByteBuffer bytes)
        {

        }

        public void read(ByteBuffer bytes)
        {
        }
    }

    private final static class Write
    extends Operation
    {
        private long destination;

        private long source;

        private int shift;

        public Write()
        {
        }

        public Write(long destination, long source, int shift)
        {
            this.destination = destination;
            this.source = source;
            this.shift = shift;
        }

        public void commit(Pager pager, Journal journal, DirtyPageMap pages)
        {
            Page toAddressPage = pager.getPage(destination, new AddressPage());
            long toPosition = ((AddressPage) toAddressPage.getPageType()).dereference(destination);
            Page toDataPage = pager.getPage(toPosition, new DataPage());

            ByteBuffer fromBytes = journal.read(source, destination);

            Allocator allocator = null;
            do
            {
                allocator = ((DataPage) toDataPage.getPageType()).getAllocator();
            }
            while (!allocator.write(toDataPage, destination, fromBytes, pages));
        }

        public void write(ByteBuffer bytes)
        {
            bytes.putShort(WRITE);
            bytes.putLong(destination);
            bytes.putLong(source);
            bytes.putInt(shift);
        }

        public void read(ByteBuffer bytes)
        {
            destination = bytes.getLong();
            source = bytes.getLong();
            shift = bytes.getInt();
        }

        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE * 2;
        }

        public ByteBuffer getByteBuffer(Pager pager, ByteBuffer bytes)
        {
            return bytes;
        }
    }

    private final static class Free
    extends Operation
    {
        private long address;

        public Free()
        {
        }

        public Free(long address)
        {
            this.address = address;
        }

        public void commit(Pager pager, Journal journal, DirtyPageMap pages)
        {
            Page addressPage = pager.getPage(address, new AddressPage());
            long referenced = ((AddressPage) addressPage.getPageType()).dereference(address);
            Page page = pager.getPage(referenced, new DataPage());
            Allocator allocator = null;
            do
            {
                allocator = ((DataPage) page.getPageType()).getAllocator();
            }
            while (!allocator.free(page, address, pages));
        }

        public void write(ByteBuffer bytes)
        {
            bytes.putShort(FREE);
            bytes.putLong(address);
        }

        public void read(ByteBuffer bytes)
        {
            address = bytes.getLong();
        }

        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE;
        }

        public ByteBuffer getByteBuffer(Pager pager, ByteBuffer bytes)
        {
            return bytes;
        }
    }

    private final static class NextOperation
    extends Operation
    {
        private long position;

        public NextOperation()
        {
        }

        public NextOperation(long position)
        {
            this.position = position;
        }

        public void write(ByteBuffer bytes)
        {
            bytes.putShort(NEXT_PAGE);
            bytes.putLong(position);
        }

        public void read(ByteBuffer bytes)
        {
            position = bytes.getLong();
        }

        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE;
        }
    }

    private final static class Commit
    extends Operation
    {
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(COMMIT);
        }

        public void read(ByteBuffer bytes)
        {
        }

        public int length()
        {
            return FLAG_SIZE;
        }

        public ByteBuffer getByteBuffer(Pager pager, ByteBuffer bytes)
        {
            return null;
        }
    }

    private static class Journal
    implements Allocator
    {
        private final Pager pager;

        private final LinkedList<Page> listOfPages;

        private final DirtyPageMap pages;

        /**
         * A table of the wilderness data pages used by the journal to create
         * temporary blocks to isolate the blocks writes of the mutator.
         */
        private final BySizeTable pagesBySize;

        private long journalStart;

        private Page journalPage;

        private final Map<Long, Long> mapOfRelocations;

        private final Map<Long, Long> mapOfVacuums;

        /**
         * Create a new journal associated with the given pager whose page
         * writes are managed by the given dirty page map. The dirty page map
         * will record the creation of wilderness pages.
         *
         * @param pager The pager of the mutator for the journal.
         * @param dirtyPageMap A dirty page map where page writes are cached
         * before being written to disk.
         */
        public Journal(Pager pager, DirtyPageMap pages)
        {
            this.journalPage = pager.newSystemPage(new JournalPage(), pages);
            this.journalStart = ((JournalPage) journalPage.getPageType()).getJournalPosition();
            this.pager = pager;
            this.listOfPages = new LinkedList<Page>();
            this.pages = pages;
            this.pagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.mapOfRelocations = new HashMap<Long, Long>();
            this.mapOfVacuums = new HashMap<Long, Long>();
        }

        public boolean free(Page page, long address, DirtyPageMap pages)
        {
            return ((DataPage) page.getPageType()).free(this, address, pages);
        }

        public synchronized boolean write(Page page, long address, ByteBuffer data, DirtyPageMap pages)
        {
            return ((DataPage) page.getPageType()).write(this, address, data, pages);
        }

        public boolean relocate(RelocatablePage from, long to)
        {
            return false;
        }

        public void relocate(Allocator allocator, Page from, Page to)
        {
        }

        public void markVacuum(long page)
        {
            mapOfVacuums.put(new Long(page), new Long(((JournalPage) journalPage.getPageType()).getJournalPosition()));
        }

        public Pager getPager()
        {
            return pager;
        }

        /**
         * Allocate a temporary block in a data page in the wilderness that
         * will isolate block writes so that they are only visible to the
         * current mutator.
         * 
         * @param blockSize
         *            The user requested block size.
         * @return The file position of the allocated block.
         */
        public long allocate(int blockSize, long address)
        {
            // Add the block overhead to the block size.
            
            int fullSize = blockSize + BLOCK_HEADER_SIZE;

            // Find a page that will fit the block from the table of data pages
            // already created by this journal.
            
            Size size = pagesBySize.bestFit(fullSize);
            if (size == null)
            {
                size = new Size(pager.newSystemPage(new DataPage(), pages));
                listOfPages.add(size.getPage());
            }

            // Adjust the size remaining for the page.

            int remaining = size.getRemaining() - fullSize;
            pagesBySize.add(new Size(size.getPage(), remaining));

            // Allocate a block from the wilderness data page.

            return ((DataPage) size.getPage().getPageType()).allocate(address, blockSize, pages);
        }

        public synchronized void rewrite(long address, ByteBuffer data)
        {
            Long position = (Long) mapOfVacuums.get(new Long(pager.getPosition(address)));
            if (position != null)
            {
                Page journalPage = pager.getPage(position.longValue(), new JournalPage());
                ((JournalPage) journalPage.getPageType()).seek(position.longValue());
                Operation operation = null;
                do
                {
                    operation = ((JournalPage) journalPage.getPageType()).next();
                    journalPage = operation.getJournalPage(pager, journalPage);
                }
                while (!operation.write(pager, address, data, pages));
            }
        }

        public long getPosition(long address, int pageSize)
        {
            return (long) Math.floor(address / pageSize);
        }

        public synchronized void unwrite(long address)
        {
            Long position = (Long) mapOfVacuums.get(new Long(pager.getPosition(address)));
            if (position != null)
            {
                Page journalPage = pager.getPage(position.longValue(), new JournalPage());
                ((JournalPage) journalPage.getPageType()).seek(position.longValue());
                Operation operation = null;
                do
                {
                    operation = ((JournalPage) journalPage.getPageType()).next();
                    journalPage = operation.getJournalPage(pager, journalPage);
                }
                while (!operation.unwrite(journalPage, address));
            }
        }

        public void shift(long position, long address, ByteBuffer bytes)
        {
            Page page = pager.getPage(position, new DataPage());
            ((DataPage) page.getPageType()).write(position, address, bytes, pages);
        }

        public void write(long position, long address, ByteBuffer bytes)
        {
            Page page = pager.getPage(position, new DataPage());
            ((DataPage) page.getPageType()).write(position, address, bytes, pages);
        }

        public ByteBuffer read(long position, long address)
        {
            Page page = pager.getPage(position, new DataPage());
            return ((DataPage) page.getPageType()).read(position, address);
        }

        public void write(Operation operation)
        {
            if (!((JournalPage) journalPage.getPageType()).write(operation, pages))
            {
                Page nextJournalPage = pager.newSystemPage(new JournalPage(), pages);
                ((JournalPage) nextJournalPage.getPageType()).next(nextJournalPage.getPosition());
                journalPage = nextJournalPage;
                write(operation);
            }
        }

        public long terminate()
        {
            for (Map.Entry<Long, Long> entry: mapOfRelocations.entrySet())
            {
                Long from = (Long) entry.getKey();
                Long to = (Long) entry.getValue();
                write(new Relocate(from.longValue(), to.longValue()));
            }
            return 0;
        }
    }

    private interface Writer
    {
        public boolean relocate(RelocatablePage from, long to);
    }

    private interface Allocator
    extends Writer
    {
        public boolean write(Page page, long address, ByteBuffer data, DirtyPageMap pages);

        public void rewrite(long address, ByteBuffer data);

        public boolean free(Page page, long address, DirtyPageMap pages);

        public void unwrite(long address);
    }

    private final static class NullAllocator
    implements Allocator
    {
        public boolean relocate(RelocatablePage from, long to)
        {
            return true;
        }

        public boolean write(Page page, long address, ByteBuffer data, DirtyPageMap pages)
        {
            return ((DataPage) page.getPageType()).write(this, address, data, pages);
        }

        public boolean free(Page page, long address, DirtyPageMap pages)
        {
            return ((DataPage) page.getPageType()).free(this, address, pages);
        }

        public void rewrite(long address, ByteBuffer data)
        {
        }

        public void unwrite(long address)
        {
        }
    }

    public final static class Mutator
    {
        private final Pager pager;
        
        private final PageLocker pageLocker;
        
        private final Journal journal;

        private final BySizeTable pagesBySize;

        private final LinkedList<Long> listOfPages;

        private final Map<Long, Long> mapOfAddresses;

        private long lastPointerPage;

        private final DirtyPageMap pages; // FIXME Rename.

        public Mutator(Pager pager, PageLocker pageLocker, Journal journal, DirtyPageMap pages)
        {
            this.pager = pager;
            this.pageLocker = pageLocker;
            this.journal = journal;
            this.pagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.listOfPages = new LinkedList<Long>();
            this.pages = pages;
            this.mapOfAddresses = new HashMap<Long, Long>();
        }

        /**
         * Allocate a block in the <code>Pack</code> to accommodate a block
         * of the specified block size. This method will reserve a new block
         * and return the address of the block. The block will not be visible
         * to other mutators until the mutator commits it's changes.
         * 
         * @param blockSize
         *            The size of the block to allocate.
         * @return The address of the block.
         */
        public long allocate(int blockSize)
        {
            // Add the header size to the block size.

            int fullSize = blockSize + BLOCK_HEADER_SIZE;

            // This is unimplemented: Creating a linked list of blocks when the
            // block size excees the size of a page.

            int pageSize = pager.getPageSize();
            if (fullSize + DATA_PAGE_HEADER_SIZE > pageSize)
            {
                // Recurse.
            }

            // If we already have a wilderness data page that will fit the
            // block, use that page. Otherwise, allocate a new wilderness data
            // page for allocation.

            Size size = pagesBySize.bestFit(fullSize);
            if (size == null)
            {
                size = new Size(pager.newSystemPage(new DataPage(), pages));
                listOfPages.add(new Long(size.getPage().getPosition()));
            }
            Page page = size.getPage();

            // Adjust the remaining size of the wilderness data page.

            int remaining = size.getRemaining() - fullSize;
            pagesBySize.add(new Size(page, remaining));

            // Reserve an address.

            Page addressPage = null;
            long address = 0L;
            do
            {
                pageLocker.acquireExclusive(singleton(lastPointerPage));
                try
                {
                    addressPage = pager.getPage(lastPointerPage, new AddressPage());
                    address = ((AddressPage) addressPage.getPageType()).reserve(pages);
                }
                finally
                {
                    pageLocker.releaseExclusive(singleton(lastPointerPage));
                }
                if (address == 0L)
                {
                    // Allocate a different page.
                    address = pager.reserve(addressPage, pages);
                }
            }
            while (address == 0L);

            long position = journal.allocate(fullSize, address);

            mapOfAddresses.put(new Long(address), new Long(position));

            journal.write(new Allocate(address, page.getPosition(), position, fullSize));

            return address;
        }

        public void commit()
        {
            // TODO This is where I left off.
            
            // First, create necessary data pages? Or vacuum.
            
            // Consolidate pages. Eliminate the need for new pages.
            Map<Long, Size> mapOfSizes = new TreeMap<Long, Size>(); 
                
            pager.pagesBySize.join(pagesBySize, mapOfSizes);
            
            // Determine the number of empty pages needed.
            
            int needed = pagesBySize.getSize() - mapOfSizes.size();

            // Obtain free pages from the available free pages in the pager.

            Set<Long> setOfEmptyPages = new HashSet<Long>();
            pager.newUserDataPages(needed, setOfEmptyPages);
            
            needed -= setOfEmptyPages.size();

            // If more pages are needed, then go and get them.

            if (needed != 0)
            {
                pager.getGoalPostLock().readLock().lock();
                try
                {
                    while (needed != 0)
                    {
                        // Get the first page in the wilderness.
                    }
                }
                finally
                {
                    pager.getGoalPostLock().readLock().unlock();
                }
            }
        }

        public ByteBuffer read(long address)
        {
            return null;
        }

        public void write(long address, ByteBuffer bytes)
        {
            // For now, the first test will write to an allocated block, so
            // the write buffer is already there.
            
            Long position = mapOfAddresses.get(address);
            if (position == null)
            {
            }
            else
            {
                journal.write(position, address, bytes);
            }
        }

        public void free(long address)
        {

        }
    }

    public static class AllocOutputStream
    extends OutputStream
    {
        private final Mutator mutator;

        private final ByteArrayOutputStream output;

        public AllocOutputStream(Mutator mutator)
        {
            this.mutator = mutator;
            this.output = new ByteArrayOutputStream();
        }

        public void write(int b)
        {
            output.write(b);
        }

        public void write(byte[] b, int off, int len)
        {
            output.write(b, off, len);
        }

        public void reset()
        {
            output.reset();
        }

        private int getSize(boolean withCount)
        {
            int size = output.size();
            if (withCount)
            {
                size += 4;
            }
            return size;
        }

        private long allocate(long address, boolean withCount)
        {
            ByteBuffer bytes = ByteBuffer.allocate(getSize(withCount));
            if (withCount)
            {
                bytes.putInt(output.size());
            }
            bytes.put(output.toByteArray());
            bytes.flip();

            mutator.write(address, bytes);

            return address;
        }

        public long allocate(boolean withCount)
        {
            return allocate(mutator.allocate(getSize(withCount)), withCount);
        }

        // public Address temporary(boolean withCount)
        // {
        // return allocate(mutator.temporary(getSize(withCount)), withCount);
        // }
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
