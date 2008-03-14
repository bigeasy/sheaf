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

    private final static short MOVE = 8;
    
    private final static short MOVED = 9;

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
    
    private static final class Moved extends Operation
    {
        @Override
        public int length()
        {
            return FLAG_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(MOVED);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
        }
    }
    private static final class Move extends Operation
    {
        private long from;
        
        private long to;
        
        private Lock lock;
        
        private Move next;

        public Move()
        {
        }

        public Move(long from, long to, Move next)
        {
            assert from != to || from == 0;
            
            Lock lock = new ReentrantLock();
            lock.lock();
            
            this.from = from;
            this.to = to;
            this.lock = lock;
            this.next = next;
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
        
        public Move getNext()
        {
            return next;
        }

        public void setNext(Move next)
        {
            assert this.next == null;

            this.next = next;
        }
        
        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 2;
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            from = bytes.getLong();
            to = bytes.getLong();
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(MOVE);
            bytes.putLong(from);
            bytes.putLong(to);
        }
    }

    private static final class Position
    {
        private final Pager pager;

        private Page page;

        private long value;

        private Reference<ByteBuffer> byteBufferReference;

        public Position(Pager pager, long value)
        {
            this.pager = pager;
            this.value = value;
        }

        private ByteBuffer load(Pager pager, long value)
        {
            int pageSize = pager.getPageSize();
            int bufferSize = pageSize;
            if (value % pageSize != 0L)
            {
                bufferSize = (int) (pageSize - value % pageSize);
            }
            ByteBuffer bytes = ByteBuffer.allocateDirect(bufferSize);
            try
            {
                pager.getFileChannel().read(bytes, value);
            }
            catch (IOException e)
            {
                throw new Danger("io.page.load", e);
            }
            bytes.clear();

            return bytes;
        }

        public void setPage(Page page)
        {
            this.page = page;
        }

        public Page getPage()
        {
            return page;
        }

        public Pager getPager()
        {
            return pager;
        }

        protected Reference<ByteBuffer> getByteBufferReference()
        {
            return byteBufferReference;
        }

        public synchronized long getValue()
        {
            return value;
        }

        protected synchronized void setValue(long value)
        {
            this.value = value;
        }

        public synchronized ByteBuffer getByteBuffer()
        {
            ByteBuffer bytes = null;
            if (byteBufferReference == null)
            {
                bytes = load(pager, value);
                byteBufferReference = new WeakReference<ByteBuffer>(bytes);
            }

            bytes = (ByteBuffer) byteBufferReference.get();
            if (bytes == null)
            {
                bytes = load(pager, value);
                byteBufferReference = new WeakReference<ByteBuffer>(bytes);
            }

            return bytes;
        }
    }

    private interface Page
    {
        public void create(Position page, DirtyPageMap pages);

        public void load(Position page);

        public boolean isInterim();
    }

    private static final class AddressPage
    implements Page
    {
        private Position position;

        private long reservations;

        public void create(Position position, DirtyPageMap pages)
        {
            ByteBuffer bytes = position.getByteBuffer();

            int capacity = bytes.capacity() / ADDRESS_SIZE;
            for (int i = 0; i < capacity; i++)
            {
                bytes.putLong(i * ADDRESS_SIZE, 0L);
            }

            pages.put(position);

            this.position = position;
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
            long firstPointer = position.getPager().getFirstPointer();
            if (position.getValue() < firstPointer)
            {
                offset = (int) firstPointer / POSITION_SIZE;
            }
            return offset + (ADDRESS_PAGE_HEADER_SIZE / POSITION_SIZE);
        }
        
        public void load(Position position)
        {
            position.setPage(this);
            this.position = position;
        }

        public boolean isInterim()
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
            synchronized (position)
            {
                int offset = (int) (address - position.getValue());
                long actual = position.getByteBuffer().getLong(offset);

                assert actual != 0L; 

                return actual;
            }
        }

        private ReservationPage getReservationPage(DirtyPageMap dirtyPages)
        {
            Pager pager = position.getPager();
            ReservationPage page = null;
            if (reservations == 0L)
            {
                page = pager.newSystemPage(new ReservationPage(), dirtyPages);
                reservations = page.getValue();
            }
            else
            {
                page = pager.getPage(reservations, new ReservationPage());
            }

            return (ReservationPage) page.getPage();
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
            synchronized (position)
            {
                // Get the reservation page. Note that the page type holds a
                // hard reference to the page. 

                ReservationPage reserver = getReservationPage(dirtyPages);

                // Get the page buffer.
                
                ByteBuffer bytes = position.getByteBuffer();

                // Iterate the page buffer looking for a zeroed address that has
                // not been reserved, reserving it and returning it if found.
                
                for (int i = getStartOffset(); i < bytes.capacity() / ADDRESS_SIZE; i++)
                {
                    if (bytes.getLong(i) == 0L && reserver.reserve(i, dirtyPages))
                    {
                        return position.getValue() + (i * ADDRESS_SIZE);
                    }
                }

                // Not found.
                
                return 0L;
            }
        }

        public long allocate()
        {
            synchronized (position)
            {
                ByteBuffer bytes = position.getByteBuffer();
                int addresses = bytes.capacity() / ADDRESS_SIZE;
                for (int i = 0; i < addresses; i++)
                {
                    if (bytes.getLong(i * ADDRESS_SIZE) > 0L)
                    {
                        bytes.putLong(i * ADDRESS_SIZE, -1L);
                        return position.getValue() + i * ADDRESS_SIZE;
                    }
                }
            }
            throw new IllegalStateException();
        }

        public void set(long address, long value)
        {
            synchronized (position)
            {
                ByteBuffer bytes = position.getByteBuffer();
                bytes.putLong((int) (address - position.getValue()), value);
            }
        }

        public long newAddress(long firstPointer)
        {
            return 0;
        }
    }

    private static class RelocatablePage
    implements Page
    {
        private Position position;
        
        private Allocator allocator;

        public void create(Position position, DirtyPageMap dirtyPages)
        {
            this.position = position;
            position.setPage(this);
        }

        public void load(Position position)
        {
            this.position = position;
            position.setPage(this);
        }

        protected Position getPosition()
        {
            return position;
        }

        public Allocator getAllocator()
        {
            synchronized (position)
            {
                return allocator;
            }
        }

        public void clearAllocator()
        {
            synchronized (position)
            {
                this.allocator = NULL_ALLOCATOR;
                position.notifyAll();
            }
        }

        public boolean isInterim()
        {
            return false;
        }

        protected void initialize(ByteBuffer bytes)
        {
        }

        public void relocate(long to)
        {
            Position position = getPosition();
            ByteBuffer bytes = position.getByteBuffer();
            FileChannel fileChannel = position.getPager().getFileChannel();
            bytes.clear();
            try
            {
                fileChannel.write(bytes, to);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            position.setValue(to);
        }
    }

    private static final class DataPage
    extends RelocatablePage
    {
        private int remaining;

        private int count;

        private boolean system;
        
        public void create(Position position, DirtyPageMap dirtyPages)
        {
            super.create(position, dirtyPages);
            
            this.count = 0;
            this.remaining = position.getPager().getPageSize() - DATA_PAGE_HEADER_SIZE; 
        }

        public void load(Position position)
        {
            ByteBuffer bytes = position.getByteBuffer();
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

        public boolean isInterim()
        {
            return system;
        }

        public int getCount()
        {
            synchronized (getPosition())
            {
                return count;
            }
        }

        public int getRemaining()
        {
            synchronized (getPosition())
            {
                return remaining;
            }
        }

        public void reset(short type)
        {
            synchronized (getPosition())
            {
                this.count = 0;
                this.remaining = getRemaining(count, getPosition().getByteBuffer());
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
            synchronized (getPosition())
            {
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
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
            synchronized (getPosition())
            {
                journal.markVacuum(getPosition().getValue());
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
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
            synchronized (getPosition())
            {
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
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
                return getPosition().getValue() + bytes.position();
            }
        }

        public long allocate(long address, int length, DirtyPageMap pages)
        {
            long position = 0L;
            synchronized (getPosition())
            {
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                int block = 0;
                while (block != count && position == 0L)
                {
                    int offset = bytes.position();
                    int size = getSize(bytes);
                    if (size > 0 && bytes.getLong(bytes.position()) == address)
                    {
                        position = getPosition().getValue() + offset;
                    }
                    else
                    {
                        bytes.position(bytes.position() + Math.abs(size));
                    }
                }

                if (position == 0L)
                {
                    position = getPosition().getValue() + bytes.position();

                    bytes.putInt(length);
                    bytes.putLong(address);

                    count++;

                    bytes.clear();
                    bytes.getLong();
                    bytes.putInt(count);
                }
            }

            pages.put(getPosition());

            return position;
        }

        public boolean write(Allocator allocator, long address, ByteBuffer data, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
                if (getAllocator() == allocator)
                {
                    allocator.rewrite(address, data);
                    ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                    if (seek(bytes, address))
                    {
                        int size = getSize(bytes);
                        bytes.putLong(address);
                        bytes.limit(size - POSITION_SIZE);
                        bytes.put(data);
                        pages.put(getPosition());
                        return true;
                    }
                }
                return false;
            }
        }

        public void write(long position, long address, ByteBuffer data, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
                bytes.clear();
                bytes.position((int) (position - getPosition().getValue()));
                int size = bytes.getInt();
                if (address != bytes.getLong())
                {
                    throw new IllegalStateException();
                }
                bytes.limit(bytes.position() + (size - BLOCK_HEADER_SIZE));
                bytes.put(data);
                pages.put(getPosition());
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
            return (int) (position - getPosition().getValue());
        }

        public boolean free(Allocator allocator, long address, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
                if (getAllocator() == allocator)
                {
                    allocator.unwrite(address);
                    ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                    if (seek(bytes, address))
                    {
                        int offset = bytes.position();

                        int size = getSize(bytes);
                        if (size > 0)
                        {
                            size = -size;
                        }
                        bytes.putInt(offset, size);

                        pages.put(getPosition());
                    }
                    return true;
                }
                return false;
            }
        }

        public void relocate(Position to, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
                getPosition().setValue(to.getValue());
                ByteBuffer bytes = to.getByteBuffer();
                for (int i = 0; i < count; i++)
                {
                    int offset = bytes.position();
                    int length = bytes.getInt();
                    long address = bytes.getLong();

                    Position addressPage = getPosition().getPager().getPage(address, new AddressPage());

                    AddressPage addressType = (AddressPage) addressPage.getPage();
                    addressType.set(address, getPosition().getValue() + offset);

                    pages.put(addressPage);

                    bytes.position(bytes.position() + length - POSITION_SIZE);
                }
            }
        }
    }

    private final static class JournalPage
    extends RelocatablePage
    {
        private int offset;

        public void create(Position page, DirtyPageMap dirtyPages)
        {
            super.create(page, dirtyPages);

            ByteBuffer bytes = getPosition().getByteBuffer();
            
            bytes.clear();
            bytes.getLong();
            bytes.putInt(-(bytes.position() + COUNT_SIZE));

            offset = bytes.position();

            getPosition().setPage(this);
        }

        public void load(Position page)
        {
            super.load(page);

            ByteBuffer bytes = getPosition().getByteBuffer();
            
            bytes.clear();
            bytes.getLong();
            
            offset = - bytes.getInt();
            
            getPosition().setPage(this);
        }

        public boolean isInterim()
        {
            return true;
        }

        public boolean write(Operation operation, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
                if (operation.length() + NEXT_PAGE_SIZE < bytes.capacity() - offset)
                {
                    bytes.position(offset);
                    operation.write(bytes);
                    pages.put(getPosition());
                    return true;
                }
                return false;
            }
        }

        public void next(long position)
        {
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
                bytes.position(offset);
                new NextOperation(position).write(bytes);
            }
        }

        public long getJournalPosition()
        {
            synchronized (getPosition())
            {
                return getPosition().getValue() + offset;
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

        public void create(Position page, DirtyPageMap pages)
        {
            super.create(page, pages);

            ByteBuffer bytes = page.getByteBuffer();

            bytes.clear();

            bytes.putLong(0L);
            bytes.putInt(-1);
            bytes.putInt(0);

            pages.put(page);
        }

        public void load(Position page)
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

        public boolean isInterim()
        {
            return true;
        }

        private int search(ByteBuffer bytes, int offset)
        {
            synchronized (getPosition())
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
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
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
                    pages.put(getPosition());
                    return true;
                }
                return false;
            }
        }

        public void remove(int offset)
        {
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
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
    extends WeakReference<Position>
    {
        private final Long position;

        public PageReference(Position page, ReferenceQueue<Position> queue)
        {
            super(page, queue);
            this.position = new Long(page.getValue());
        }

        public Long getPosition()
        {
            return position;
        }
    }

    private final static class Size
    {
        private final Position page;

        private final int remaining;

        public Size(Position page)
        {
            this.page = page;
            this.remaining = ((DataPage) page.getPage()).getRemaining();
        }

        public Size(Position page, int size)
        {
            this.page = page;
            this.remaining = size;
        }

        public Position getPage()
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
                
                assert value != null && value[0] != 0;
                
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
                
                assert value == null;
                
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
        
        public void releaseExclusive(Set<Long> setOfPages)
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

    private interface MoveRecorder
    {
        public void record(Move move);
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

        public void add(Position page)
        {
            add(new Size(page, ((DataPage) page.getPage()).getRemaining()));
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

        public synchronized void join(BySizeTable pagesBySize, Map<Long, Size> mapOfSizes)
        {
            for (List<Size> listOfSizes: pagesBySize.listOfListsOfSizes)
            {
                for(Size size: listOfSizes)
                {
                    Size found = bestFit(size.getSize());
                    if (found != null)
                    {
                        mapOfSizes.put(size.getPage().getValue(), found);
                    }
                }
            }
        }
    }
    
    private interface Returnable<T>
    {
        public T run();
    }
    
    private final static class MoveList
    {
        private final MoveRecorder recorder;

        private final ReadWriteLock readWriteLock;

        private Move headOfMoves;
        
        public MoveList()
        {
            this.recorder = null;
            this.headOfMoves = new Move(0, 0, null);
            this.readWriteLock = new ReentrantReadWriteLock();
        }

        public MoveList(MoveRecorder recorder, MoveList listOfMoves)
        {
            this.recorder = recorder;
            this.headOfMoves = listOfMoves.headOfMoves;
            this.readWriteLock = listOfMoves.readWriteLock;
        }
        public void add(Move move)
        {
            readWriteLock.writeLock().lock();
            try
            {
                Move iterator = headOfMoves;
                while (iterator.getNext() != null)
                {
                    iterator = iterator.getNext();
                }
                iterator.setNext(move);
                
                headOfMoves = iterator;
            }
            finally
            {
                readWriteLock.writeLock().unlock();
            }
        }
        
        public <T> T mutate(Returnable<T> returnable)
        {
            for (;;)
            {
                readWriteLock.readLock().lock();
                try
                {
                    if (headOfMoves.next == null)
                    {
                        return returnable.run();
                    }
                    else
                    {
                        headOfMoves = headOfMoves.next;
                        headOfMoves.getLock().lock();
                        headOfMoves.getLock().unlock();
                        recorder.record(headOfMoves);
                    }
                }
                finally
                {
                    readWriteLock.readLock().unlock();
                }
            }
        }
        
        public void mutate(Runnable runnable)
        {
            for (;;)
            {
                readWriteLock.readLock().lock();
                try
                {
                    if (headOfMoves.next == null)
                    {
                        runnable.run();
                        break;
                    }
                    else
                    {
                        headOfMoves = headOfMoves.next;
                        headOfMoves.getLock().lock();
                        headOfMoves.getLock().unlock();
                        recorder.record(headOfMoves);
                    }
                }
                finally
                {
                    readWriteLock.readLock().unlock();
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
        
        /**
         * A read/write lock that protects the end of the move list.
         */
        private final ReadWriteLock moveListLock;
        
        /**
         * A lock to ensure that only one mutator at a time is moving pages in
         * the interim page area.
         */
        private final Lock moveLock;

        /**
         * A read/write lock that protects the boundary below which pages in the
         * interim page area are locked for relocation and for commit.
         */
        private final ReadWriteLock goalPostLock;

        private final ReferenceQueue<Position> queue;

        public final File file;

        public final Map<URI, Long> mapOfStaticPages;

        private final int alignment;

        public final BySizeTable pagesBySize;

        private final Map<Long, Position> mapOfPointerPages;

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

        private long goalPost;
            
        private final MoveList listOfMoves;
        
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
            this.journalBuffer = journalBuffer;
            this.mapOfPointerPages = new HashMap<Long, Position>();
            this.pagesBySize = new BySizeTable(pageSize, alignment);
            this.mapOfStaticPages = mapOfStaticPages;
            this.pointerPageCountBytes = ByteBuffer.allocateDirect(COUNT_SIZE);
            this.setOfFreeUserPages = new TreeSet<Long>();
            this.setOfFreeInterimPages = new TreeSet<Long>(new Reverse<Long>());
            this.queue = new ReferenceQueue<Position>();
            this.goalPostLock = new ReentrantReadWriteLock();
            this.goalPost = firstSystemPage;
            this.moveListLock = new ReentrantReadWriteLock();
            this.moveLock = new ReentrantLock();
            this.listOfMoves = new MoveList();
        }
        
        public long getFirstPointer()
        {
            return firstPointer;
        }
        
        public Lock getMoveLock()
        {
            return moveLock;
        }
        
        public ReadWriteLock getGoalPostLock()
        {
            return goalPostLock;
        }
        
        public ReadWriteLock getMoveListLock()
        {
            return moveListLock;
        }
        
        public synchronized long getFirstInterimPage()
        {
            return firstSystemPage;
        }

        public synchronized void setFirstInterimPage(long firstInterimPage)
        {
            this.firstSystemPage = firstInterimPage;
        }
        
        public long getGoalPost()
        {
            return goalPost;
        }
        
        public void setGoalPost(long goalPost)
        {
            this.goalPost = goalPost;
        }
        
        public MoveList getMoveList()
        {
            return listOfMoves;
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
                        Position page = getPage(firstPointerPage, new AddressPage());
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

        public synchronized long reserve(Position page, DirtyPageMap pages)
        {
            long address = ((AddressPage) page.getPage()).reserve(pages);
            if (address == 0L)
            {
                mapOfPointerPages.remove(new Long(page.getValue()));
            }
            return address;
        }

        public synchronized Position getAddressPage(long position, DirtyPageMap pages)
        {
            Position addressPage = null;
            if (mapOfPointerPages.size() == 0)
            {
                Journal journal = new Journal(this, new DirtyPageMap(this, 16));
                long firstDataPage = 0L;
                Position fromDataPage = getPage(firstDataPage, new DataPage());
                Position toDataPage = newDataPage(pages);
                Allocator allocator = ((DataPage) fromDataPage.getPage()).getAllocator();
                journal.relocate(allocator, fromDataPage, toDataPage);
            }
            if (position != 0L)
            {
                addressPage = getPage(position, new AddressPage());
            }
            if (addressPage == null)
            {
                addressPage = (Position) mapOfPointerPages.values().iterator().next();
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

        public Position newDataPage(DirtyPageMap pages)
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
            Position page = null;
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
                                page = new Position(this, firstSystemPage);
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

        private Position getPageByPosition(long position)
        {
            Position page = null;
            Long boxPosition = new Long(position);
            PageReference chunkReference = (PageReference) mapOfPagesByPosition.get(boxPosition);
            if (chunkReference != null)
            {
                page = (Position) chunkReference.get();
            }
            return page;
        }

        private Position removePageByPosition(long position)
        {
            PageReference existing = (PageReference) mapOfPagesByPosition.get(new Long(position));
            Position p = null;
            if (existing != null)
            {
                p = existing.get();
                existing.enqueue();
                collect();
            }
            return p;
        }

        private void addPageByPosition(Position page)
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

        public Page getPage(long value, Page page)
        {
            Position position = null;
            synchronized (mapOfPagesByPosition)
            {
                value = (long) Math.floor(value - (value % pageSize));
                position = getPageByPosition(value);
                if (position == null)
                {
                    position = new Position(this, value);
                    page.load(position);
                    addPageByPosition(position);
                }
                // FIXME Is this thread safe?
                else if(position.getPage().getClass() != page.getClass())
                {
                    page.load(position);
                }
            }
            synchronized (position)
            {
                if (!position.getPage().getClass().equals(page.getClass()))
                {
                    page.load(position);
                }
            }
            return position.getPage();
        }

        public Page newSystemPage(Page pageType, DirtyPageMap pages)
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
            // then a moving thread might be attempting to create a relocateble
            // page and moving the page. We need to lock it!?

            Position page = new Position(this, position);

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
        
        public long newStaticInterimPage()
        {
            long position = 0L;
            synchronized (setOfFreeInterimPages)
            {
                if (setOfFreeInterimPages.size() != 0)
                {
                    position = setOfFreeInterimPages.last();
                    if (position < goalPost)
                    {
                        setOfFreeInterimPages.remove(position);
                    }
                }
            }
            if (position < goalPost)
            {
                position = fromWilderness();
            }
            return position;
        }
        
        public boolean removeInterimPageIfFree(long position)
        {
            synchronized (setOfFreeInterimPages)
            {
                if (setOfFreeInterimPages.contains(position))
                {
                    setOfFreeUserPages.remove(position);
                    return true;
                }
            }
            return false;
        }

        public void relocate(Move head)
        {
            synchronized (mapOfPagesByPosition)
            {
                Position position = removePageByPosition(head.getFrom());
                if (position != null)
                {
                    assert head.getTo() == position.getValue();
                    addPageByPosition(position);
                }
            }
        }
    }

    public final static class DirtyPageMap
    {
        private final Pager pager;

        private final Map<Long, Position> mapOfPages;

        private final Map<Long, ByteBuffer> mapOfByteBuffers;

        private final int capacity;

        public DirtyPageMap(Pager pager, int capacity)
        {
            this.pager = pager;
            this.mapOfPages = new HashMap<Long, Position>();
            this.mapOfByteBuffers = new HashMap<Long, ByteBuffer>();
            this.capacity = capacity;
        }

        public void put(Position page)
        {
            mapOfPages.put(page.getValue(), page);
            mapOfByteBuffers.put(page.getValue(), page.getByteBuffer());
            if (mapOfPages.size() > capacity)
            {
                flush();
            }
        }

        public void flush()
        {
            FileChannel fileChannel = pager.getFileChannel();
            for (Position page: mapOfPages.values())
            {
                synchronized (page)
                {
                    ByteBuffer bytes = page.getByteBuffer();
                    bytes.clear();
                    try
                    {
                        fileChannel.write(bytes, page.getValue());
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

        public Position getJournalPage(Pager pager, Position page)
        {
            return page;
        }

        public boolean write(Pager pager, long destination, ByteBuffer data, DirtyPageMap pages)
        {
            return false;
        }

        public boolean unwrite(Position journalPage, long destination)
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
            this.position = position;
            this.length = length;
        }

        public void commit(Pager pager, Journal journal, DirtyPageMap pages)
        {
            DataPage dataPage = (DataPage) pager.getPage(page, new DataPage()).getPage();
            long location = dataPage.allocate(address, length, pages);
            DataPage interimPage = (DataPage) pager.getPage(position, new DataPage()).getPage();
            ByteBuffer bytes = interimPage.read(position, address);
            dataPage.write(location, address, bytes, pages);
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
                Position page = pager.getPage(source, new DataPage());
                ((DataPage) page.getPage()).write(source, address, data, pages);
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
        private int offset;
        
        private long source;
        
        private long destination;

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
            Position toAddressPage = pager.getPage(destination, new AddressPage());
            long toPosition = ((AddressPage) toAddressPage.getPage()).dereference(destination);
            Position toDataPage = pager.getPage(toPosition, new DataPage());

            ByteBuffer fromBytes = journal.read(source, destination);

            Allocator allocator = null;
            do
            {
                allocator = ((DataPage) toDataPage.getPage()).getAllocator();
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

    private final static class MoveEntry
    extends Operation
    {
        @Override
        public int length()
        {
            return 0;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
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
            Position addressPage = pager.getPage(address, new AddressPage());
            long referenced = ((AddressPage) addressPage.getPage()).dereference(address);
            Position page = pager.getPage(referenced, new DataPage());
            Allocator allocator = null;
            do
            {
                allocator = ((DataPage) page.getPage()).getAllocator();
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

        private final LinkedList<Position> listOfPages;

        private final DirtyPageMap dirtyPages;

        /**
         * A table of the wilderness data pages used by the journal to create
         * temporary blocks to isolate the blocks writes of the mutator.
         */
        private final BySizeTable pagesBySize;

        private long journalStart;

        private JournalPage journalPage;

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
            this.journalPage = (JournalPage) pager.newSystemPage(new JournalPage(), pages).getPage();
            this.journalStart = journalPage.getJournalPosition();
            this.pager = pager;
            this.listOfPages = new LinkedList<Position>();
            this.dirtyPages = pages;
            this.pagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.mapOfVacuums = new HashMap<Long, Long>();
        }
        
        public long getJournalStart()
        {
            return journalStart;
        }
        
        public long getJournalPosition()
        {
            return journalPage.getJournalPosition();
        }

        public boolean free(Position page, long address, DirtyPageMap pages)
        {
            return ((DataPage) page.getPage()).free(this, address, pages);
        }

        public synchronized boolean write(Position page, long address, ByteBuffer data, DirtyPageMap pages)
        {
            return ((DataPage) page.getPage()).write(this, address, data, pages);
        }

        public boolean relocate(RelocatablePage from, long to)
        {
            return false;
        }

        public void relocate(Allocator allocator, Position from, Position to)
        {
        }

        public void markVacuum(long page)
        {
            mapOfVacuums.put(new Long(page), new Long(journalPage.getJournalPosition()));
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
                size = new Size(pager.newSystemPage(new DataPage(), dirtyPages));
                listOfPages.add(size.getPage());
            }

            // Adjust the size remaining for the page.

            int remaining = size.getRemaining() - fullSize;
            pagesBySize.add(new Size(size.getPage(), remaining));

            // Allocate a block from the wilderness data page.

            return ((DataPage) size.getPage().getPage()).allocate(address, blockSize, dirtyPages);
        }

        public synchronized void rewrite(long address, ByteBuffer data)
        {
            Long position = (Long) mapOfVacuums.get(new Long(pager.getPosition(address)));
            if (position != null)
            {
                Position journalPage = pager.getPage(position.longValue(), new JournalPage());
                ((JournalPage) journalPage.getPage()).seek(position.longValue());
                Operation operation = null;
                do
                {
                    operation = ((JournalPage) journalPage.getPage()).next();
                    journalPage = operation.getJournalPage(pager, journalPage);
                }
                while (!operation.write(pager, address, data, dirtyPages));
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
                Position journalPage = pager.getPage(position.longValue(), new JournalPage());
                ((JournalPage) journalPage.getPage()).seek(position.longValue());
                Operation operation = null;
                do
                {
                    operation = ((JournalPage) journalPage.getPage()).next();
                    journalPage = operation.getJournalPage(pager, journalPage);
                }
                while (!operation.unwrite(journalPage, address));
            }
        }

        public void shift(long position, long address, ByteBuffer bytes)
        {
            Position page = pager.getPage(position, new DataPage());
            ((DataPage) page.getPage()).write(position, address, bytes, dirtyPages);
        }

        public void write(long position, long address, ByteBuffer bytes)
        {
            Position page = pager.getPage(position, new DataPage());
            ((DataPage) page.getPage()).write(position, address, bytes, dirtyPages);
        }

        public ByteBuffer read(long position, long address)
        {
            Position page = pager.getPage(position, new DataPage());
            return ((DataPage) page.getPage()).read(position, address);
        }

        public void write(Operation operation)
        {
            if (!journalPage.write(operation, dirtyPages))
            {
                JournalPage nextJournalPage = (JournalPage) pager.newSystemPage(new JournalPage(), dirtyPages).getPage();
                journalPage.write(new NextOperation(nextJournalPage.getJournalPosition()), dirtyPages);
                journalPage = nextJournalPage;
                write(operation);
            }
        }
    }

    private interface Writer
    {
        public boolean relocate(RelocatablePage from, long to);
    }

    private interface Allocator
    extends Writer
    {
        public boolean write(Position page, long address, ByteBuffer data, DirtyPageMap pages);

        public void rewrite(long address, ByteBuffer data);

        public boolean free(Position page, long address, DirtyPageMap pages);

        public void unwrite(long address);
    }

    private final static class NullAllocator
    implements Allocator
    {
        public boolean relocate(RelocatablePage from, long to)
        {
            return true;
        }

        public boolean write(Position page, long address, ByteBuffer data, DirtyPageMap pages)
        {
            return ((DataPage) page.getPage()).write(this, address, data, pages);
        }

        public boolean free(Position page, long address, DirtyPageMap pages)
        {
            return ((DataPage) page.getPage()).free(this, address, pages);
        }

        public void rewrite(long address, ByteBuffer data)
        {
        }

        public void unwrite(long address)
        {
        }
    }
    
    public final static class Mutator implements MoveRecorder
    {
        private final Pager pager;
        
        private final PageLocker pageLocker;
        
        private final Journal journal;

        private final BySizeTable pagesBySize;

        private final Set<Long> setOfPages;

        private final Map<Long, Long> mapOfAddresses;

        private long lastPointerPage;

        private final DirtyPageMap dirtyPages;
        
        private final List<Move> listOfMoved;
        
        private final MoveList listOfMoves;

        public Mutator(Pager pager, PageLocker pageLocker, Journal journal, DirtyPageMap dirtyPages)
        {
            this.pager = pager;
            this.pageLocker = pageLocker;
            this.journal = journal;
            this.pagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.setOfPages = new HashSet<Long>();
            this.dirtyPages = dirtyPages;
            this.mapOfAddresses = new HashMap<Long, Long>();
            this.listOfMoved = new LinkedList<Move>();
            this.listOfMoves = new MoveList(this, pager.getMoveList());
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
                size = new Size(pager.newSystemPage(new DataPage(), dirtyPages));
                setOfPages.add(new Long(size.getPage().getValue()));
            }
            Position page = size.getPage();

            // Adjust the remaining size of the wilderness data page.

            int remaining = size.getRemaining() - fullSize;
            pagesBySize.add(new Size(page, remaining));

            // Reserve an address.

            Position addressPage = null;
            long address = 0L;
            do
            {
                pageLocker.acquireExclusive(singleton(lastPointerPage));
                try
                {
                    addressPage = pager.getPage(lastPointerPage, new AddressPage());
                    address = ((AddressPage) addressPage.getPage()).reserve(dirtyPages);
                }
                finally
                {
                    pageLocker.releaseExclusive(singleton(lastPointerPage));
                }
                if (address == 0L)
                {
                    // Allocate a different page.
                    address = pager.reserve(addressPage, dirtyPages);
                }
            }
            while (address == 0L);

            long position = journal.allocate(fullSize, address);

            mapOfAddresses.put(new Long(address), new Long(position));

            journal.write(new Allocate(address, page.getValue(), position, fullSize));

            return address;
        }

        public void commit()
        {
            // First, create necessary data pages? Or vacuum.
            
            // Consolidate pages. Eliminate the need for new pages.

            final Map<Long, Size> mapOfSizes = new TreeMap<Long, Size>(); 
            pager.pagesBySize.join(pagesBySize, mapOfSizes);
            
            // Determine the number of empty pages needed.
            
            int needed = setOfPages.size() - mapOfSizes.size();
            
            // Obtain free pages from the available free pages in the pager.

            final Set<Long> setOfEmptyPages = new HashSet<Long>();
            pager.newUserDataPages(needed, setOfEmptyPages);

            needed -= setOfEmptyPages.size();

            // If more pages are needed, then go and get them.

            // FIXME Remember that you do not need to record the moves yet,
            // just add your own. Should not be a problem.
            while (needed != 0)
            {
                needed = move(needed, null);
            }
            
            listOfMoves.mutate(new Runnable()
            {
                public void run()
                {
                    // Grab the position. This is where we start.
                    long position = journal.getJournalPosition();
                    
                    // Create the list of moves.
                    for (Move move: listOfMoved)
                    {
                        record(move);
                    }
                    
                    // Create an operation for all the vacuums.
                    for (Map.Entry<Long, Size> entry: mapOfSizes.entrySet())
                    {
                        DataPage dataPage = (DataPage) pager.getPage(entry.getKey(), new DataPage()).getPage();
                        journal.write(new Vacuum());
                    }
                    
                    // Insert a break, so that we can run vacuums ahead of
                    // everything else during recovery.
                    
                    // TODO Abstract journal replay out, so it can be used
                    // here and during recovery.
                    
                    // Create a next pointer to point at the start of operations.
                    
                    // Obtain a journal header and record the head.
                    
                    // First do the vacuums.
                    
                    // Then do everything else.
                }
            });
        }
    
        /**
         * Iterate through the pages in the interim area up to the relocatable
         * page boundary.
         * <p>
         * If the page is in the list of free interim pages, remove it. We
         * will not lock it. No other mutator will reference a free page
         * because no other mutator is moving pages and no other mutator will
         * be using it for work space.
         * <p>
         * If the page is not in the list of free interim pages, we do have to
         * lock it.
         */
        private int gatherPages(int needed, SortedSet<Long> setOfInUse, Set<Long> setOfFree)
        {
            long position = pager.getFirstInterimPage();

            // FIXME Deadlock of locking of page interleaves with
            // locking of list.

            int acquired = 0;
            long goalPost = pager.getGoalPost();
            while (position < goalPost && acquired < needed)
            {
                if (pager.removeInterimPageIfFree(position))
                {
                    setOfFree.add(position);
                }
                else
                {
                    setOfInUse.add(position);
                }
                acquired++;
                position += pager.getPageSize();
            }
            
            return acquired;
        }
        
        private void move(Move head)
        {
            while (head != null)
            {
                RelocatablePage page = (RelocatablePage) pager.getPage(head.getFrom(), new RelocatablePage()).getPage();
                page.relocate(head.getTo());
                // TODO Curious about this. Will this get out of sync? Do I have
                // things locked down enough?
                
                // Need to look at each method in mutator and assure myself that
                // no one else is reading the pager's page map. The
                // syncrhoniation of the page map is needed between mutators for
                // ordinary tasks. Here, though, everyone else should be all
                // locked down.
                pager.relocate(head);
                head = head.getNext();
            }  
        }

        private int move(int needed, Set<Long> setOfFreePages)
        {
            // FIXME Probably need to lock the move list. It cannot change. But
            // then this lock locks out everyone else from locking the move
            // list. So, maybe more liveness for other processes who are just
            // writing.
            pager.getMoveLock().lock();
            SortedSet<Long> setOfInUse = new TreeSet<Long>();
            SortedSet<Long> setOfLocked = new TreeSet<Long>();
            Set<Long> setOfFree = new HashSet<Long>();
            pager.getGoalPostLock().readLock().lock();
            try
            {
                // FIXME Loop?

                // Get the first page in the wilderness.

                int acquired = gatherPages(needed, setOfInUse, setOfFree);

                // For the set of pages in use, add the page to the move list.

                Move head = null;
                Move move = null;
                Iterator<Long> inUse = setOfInUse.iterator();
                if (inUse.hasNext())
                {
                    long from = inUse.next();
                    head = move = new Move(from, pager.newStaticInterimPage(), null);
                }
                while (inUse.hasNext())
                {
                    long from = inUse.next();
                    move = new Move(from, pager.newStaticInterimPage(), move);
                }

                if (head != null)
                {
                    pager.getMoveList().add(move);
                }
                
                // At this point, no one else is moving because we have a
                // monitor that only allows one mutator to move at once. Other
                // mutators may be referencing pages that are moved. Appending
                // to the move list blocked referencing mutators with a latch on
                // each move. We are clear to move the pages that are in use.

                if (head != null)
                {
                    move(head);
                }

                needed -= acquired;

                // FIXME Do we move the goal post here? Or do we move it when we
                // get back? I mean, does it matter if two people move the goal
                // post? We'll both add by the same amount.
                
                // FIXME Okay. Looks like locking the relocatable pages doesn't
                // matter since any other commit is going to wait on completion
                // of the move list. The move list is getting confusing.
                
                // We can mark pages as interim in the move list, then try to
                // convince yourself that we can determine when our interim
                // pages have moved. We're trying to avoid the case where a huge
                // move will lock a lot of pages, but we want liveness for small
                // moves that move only a few pages. This may be falling out of
                // the move list. A list I have to maintain anyway.

                if (needed != 0)
                {
                    pager.getGoalPostLock().readLock().unlock();
                    try
                    {
                        pager.getGoalPostLock().writeLock().lock();
                        try
                        {
                            pager.setGoalPost(pager.getGoalPost() + pager.getPageSize() * (long) (needed * 1.25));
                        }
                        finally
                        {
                            pager.getGoalPostLock().writeLock().unlock();
                        }
                    }
                    finally
                    {
                        pager.getGoalPostLock().readLock().lock();
                    }
                }
            }
            finally
            {
                pageLocker.releaseExclusive(setOfLocked);
                pager.getGoalPostLock().readLock().unlock();
                pager.getMoveLock().unlock();
            }
            return needed;
        }

        public ByteBuffer read(long address)
        {
            return null;
        }

        public void write(final long address, final ByteBuffer bytes)
        {
            listOfMoves.mutate(new Runnable()
            {
                public void run()
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
            });
        }

        public void free(final long address)
        {
            listOfMoves.mutate(new Runnable()
            {
                public void run()
                {
                    
                }
            });
        }
        
        public void record(Move move)
        {
            listOfMoved.add(move);
            record(new Moved());
        }

        public void record(Operation operation)
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
