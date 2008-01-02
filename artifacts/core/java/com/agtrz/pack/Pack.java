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
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public class Pack
{
    private final static int FLAG_SIZE = 2;

    private final static int COUNT_SIZE = 4;

    private final static int POSITION_SIZE = 8;

    private final static int CHECKSUM_SIZE = 8;

    private final static int ADDRESS_SIZE = 8;

    private final static int FILE_HEADER_SIZE = COUNT_SIZE * 6 + ADDRESS_SIZE;

    private final static int PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE + FLAG_SIZE;

    private final static int BLOCK_HEADER_SIZE = POSITION_SIZE + COUNT_SIZE;

    private final static short ALLOCATE = 1;

    private final static short WRITE = 2;

    private final static short FREE = 3;

    private final static short NEXT_PAGE = 4;

    private final static short COMMIT = 5;

    private final static short VACUUM = 6;

    private final static short SHIFT = 7;

    private final static short RELOCATE = 8;

    private final static short ROLLBACK = 0;

    private final static int NEXT_PAGE_SIZE = FLAG_SIZE + ADDRESS_SIZE;

    private final static int ADDRESS_PAGE_HEADER_SIZE = CHECKSUM_SIZE + POSITION_SIZE;

    private final static NullAllocator NULL_ALLOCATOR = new NullAllocator();

    private final Pager pager;

    public Pack(Pager pager)
    {
        this.pager = pager;
    }

    public Mutator mutate()
    {
        PageMap pages = new PageMap(pager, 16);
        return new Mutator(pager, new Journal(pager, pages), pages);
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
        private final Map mapOfStaticPageSizes;

        private int pageSize;

        private int alignment;

        private int internalJournalCount;

        public Creator()
        {
            this.mapOfStaticPageSizes = new TreeMap();
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

        public Pack create(File file)
        {
            FileChannel fileChannel = newFileChannel(file);
            ByteBuffer header = ByteBuffer.allocateDirect(FILE_HEADER_SIZE);

            int pointerPageCount = 1;

            header.putInt(pageSize);
            header.putInt(alignment);
            header.putLong(0L);
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

            Map mapOfStaticPages = new HashMap();

            long firstPointer = FILE_HEADER_SIZE + ((internalJournalCount) * POSITION_SIZE);

            Pack pack = new Pack(new Pager(file, fileChannel, pageSize, alignment, mapOfStaticPages, journals, firstPointer, pointerPageCount));

            Mutator mutator = pack.mutate();

            Iterator entries = mapOfStaticPageSizes.entrySet().iterator();
            while (entries.hasNext())
            {
                Map.Entry entry = (Map.Entry) entries.next();
                URI uri = (URI) entry.getKey();
                Integer size = (Integer) entry.getValue();
                long address = mutator.allocate(size.intValue());
                mapOfStaticPages.put(uri, new Long(address));
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

            return new Pack(new Pager(file, fileChannel, pageSize, alignment, mapOfStaticPages, journals, firstPointer, pointerPageCount));
        }
    }

    private static abstract class Page
    {
        private final Pager pager;

        private long position;

        private Reference bufferReference;

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
                long nextPosition = (long) Math.floor(position + pageSize / pageSize);
                bufferSize = (int) (nextPosition - position);
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

        public void initialize(long position)
        {
            initialize(load(getPager(), position));
        }

        protected abstract void initialize(ByteBuffer bytes);

        protected Pager getPager()
        {
            return pager;
        }

        public long getPosition()
        {
            return position;
        }

        protected void setPosition(long position)
        {
            this.position = position;
        }

        public synchronized ByteBuffer getByteBuffer()
        {
            if (bufferReference == null)
            {
                ByteBuffer bytes = load(pager, position);
                bufferReference = new WeakReference(bytes);
            }

            ByteBuffer bytes = (ByteBuffer) bufferReference.get();
            if (bytes == null)
            {
                bytes = load(pager, position);
                bufferReference = new WeakReference(bytes);
            }

            return bytes;
        }
    }

    private static final class AddressPage
    extends Page
    {
        private int capacity;

        private int count;

        private int reserved;

        private long reservations;

        public AddressPage(Pager pager, long position)
        {
            super(pager, position);

            ByteBuffer bytes = getByteBuffer();

            capacity = bytes.capacity() / ADDRESS_SIZE;
            for (int i = 0; i < capacity; i++)
            {
                if (bytes.getLong(i * ADDRESS_SIZE) == 0L)
                {
                    count++;
                }
            }

            reserved = count;
        }

        protected void initialize(ByteBuffer bytes)
        {
            bytes.putLong(0L);
            bytes.putLong(0L);
        }

        public synchronized int available()
        {
            return capacity - reserved;
        }

        public synchronized long dereference(long address)
        {
            int offset = (int) (address - getPosition());
            return getByteBuffer().getLong(offset);
        }

        public synchronized long reserve(PageMap pages)
        {
            if (reservations == 0L)
            {
                reservations = getPager().newTemporaryPage(new ReservationPage(getPager(), 0L));
            }
            ReservationPage reservationPage = getPager().getReservationPage(reservations);

            if (count == 0)
            {
                return 0L;
            }

            ByteBuffer bytes = getByteBuffer();
            int addresses = (bytes.capacity() - ADDRESS_PAGE_HEADER_SIZE) / ADDRESS_SIZE;
            for (int i = 0; i < addresses; i++)
            {
                int index = ADDRESS_PAGE_HEADER_SIZE + (i * ADDRESS_SIZE);
                if (bytes.getLong(index) < 0L && reservationPage.reserve(i, pages))
                {
                    return getPosition() + ADDRESS_PAGE_HEADER_SIZE + (i * ADDRESS_SIZE);
                }
            }

            return 0L;
        }

        public synchronized void cancelReservations(int count)
        {
            reserved -= count;
        }

        public synchronized long allocate()
        {
            if (count == 0)
            {
                return 0L;
            }

            ByteBuffer bytes = getByteBuffer();
            int addresses = bytes.capacity() / ADDRESS_SIZE;
            for (int i = 0; i < addresses; i++)
            {
                if (bytes.getLong(i * ADDRESS_SIZE) > 0L)
                {
                    count++;
                    bytes.putLong(i * ADDRESS_SIZE, -1L);
                    return getPosition() + i * ADDRESS_SIZE;
                }
            }

            throw new IllegalStateException();
        }

        public synchronized void set(long address, long position)
        {
            ByteBuffer bytes = getByteBuffer();
            bytes.putLong((int) (address - getPosition()), position);
        }

        public long newAddress(long firstPointer)
        {
            return 0;
        }
    }

    private interface BlockWriter
    {
        public void write(ByteBuffer block, ByteBuffer data);
    }

    public final static class UserBlockWriter
    implements BlockWriter
    {
        private final long address;

        public UserBlockWriter(long address)
        {
            this.address = address;
        }

        public void write(ByteBuffer block, ByteBuffer data)
        {
            block.putLong(address);
            if (data.remaining() > block.remaining())
            {
                data.limit(data.position() + block.remaining());
            }
            block.put(data);
        }
    }

    public final static class JournalBlockWriter
    implements BlockWriter
    {
        public void write(ByteBuffer block, ByteBuffer data)
        {
            if (data.remaining() > block.remaining())
            {
                data.limit(data.position() + block.remaining());
            }
            block.put(data);
        }
    }

    private interface BlockReader
    {
        public ByteBuffer read(ByteBuffer block);
    }

    private final static class UserBlockReader
    implements BlockReader
    {
        private final long address;

        public UserBlockReader(long address)
        {
            this.address = address;
        }

        public ByteBuffer read(ByteBuffer block)
        {
            if (block.getLong() != address)
            {
                throw new ConcurrentModificationException();
            }
            ByteBuffer data = ByteBuffer.allocate(block.remaining());
            data.put(block);
            return data;
        }
    }

    private final static class JournalBlockReader
    implements BlockReader
    {
        public ByteBuffer read(ByteBuffer block)
        {
            return block.slice(); // FIXME Problem with that?
        }
    }

    private static abstract class RelocatablePage
    extends Page
    {
        public RelocatablePage(Pager pager, long position)
        {
            super(pager, position);
        }
    }

    private static final class DataPage
    extends RelocatablePage
    {
        private int remaining;

        private int count;

        private short type;

        private Allocator allocator;

        public DataPage(Pager pager, long position)
        {
            super(pager, position);
        }

        public DataPage(Pager pager, long position, short type)
        {
            super(pager, position);
            this.type = type;
        }

        public synchronized boolean setAllocator(Allocator allocator, boolean wait)
        {
            if (this.allocator == NULL_ALLOCATOR)
            {
                this.allocator = allocator;
            }
            if (this.allocator == allocator)
            {
                return true;
            }
            if (wait)
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
            return false;
        }

        public synchronized Allocator getAllocator()
        {
            return allocator;
        }

        public synchronized void clearAllocator()
        {
            this.allocator = NULL_ALLOCATOR;
            notifyAll();
        }

        protected void initialize(ByteBuffer bytes)
        {
            this.count = bytes.getInt();
            this.remaining = getRemaining(count, bytes);
            this.type = bytes.getShort();
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

        public synchronized int getCount()
        {
            return count;
        }

        public synchronized int getRemaining()
        {
            return remaining;
        }

        public synchronized short getType()
        {
            return type;
        }

        public synchronized void reset(short type)
        {
            this.count = 0;
            this.remaining = getRemaining(count, getByteBuffer());
            this.type = type;
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

        public synchronized ByteBuffer read(long position, BlockReader reader)
        {
            ByteBuffer bytes = getBlockRange(getByteBuffer());
            int offset = getOffset(position);

            if (seek(bytes, offset))
            {
                int size = getSize(bytes);
                bytes.limit(bytes.position() + size);
                return reader.read(bytes);
            }

            throw new ArrayIndexOutOfBoundsException();
        }

        public synchronized void vacuum(Journal journal, PageMap pages)
        {
            journal.markVacuum(getPosition());
            ByteBuffer bytes = getBlockRange(getByteBuffer());
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

                    long fromPosition = journal.allocate(size);
                    journal.write(fromPosition, fromBuffer);

                    journal.write(new Shift(address, fromPosition));
                }
            }
        }

        public synchronized long allocate(int length, PageMap pages)
        {
            ByteBuffer bytes = getBlockRange(getByteBuffer());
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
            return getPosition() + bytes.position();
        }

        public synchronized long allocate(long address, int length, PageMap pages)
        {
            ByteBuffer bytes = getBlockRange(getByteBuffer());
            long position = 0L;
            int block = 0;
            while (block != count && position == 0L)
            {
                int offset = bytes.position();
                int size = getSize(bytes);
                if (size > 0 && bytes.getLong(bytes.position()) == address)
                {
                    position = getPosition() + offset;
                }
                else
                {
                    bytes.position(bytes.position() + Math.abs(size));
                }
            }

            if (position == 0L)
            {
                position = getPosition() + bytes.position();

                bytes.putInt(length);
                bytes.putLong(address);

                count++;

                bytes.clear();
                bytes.getLong();
                bytes.putInt(count);
            }

            pages.put(this);

            return position;
        }

        public boolean write(Allocator allocator, long address, ByteBuffer data, PageMap pages)
        {
            if (this.allocator == allocator)
            {
                allocator.rewrite(address, data);
                ByteBuffer bytes = getBlockRange(getByteBuffer());
                if (seek(bytes, address))
                {
                    int size = getSize(bytes);
                    bytes.putLong(address);
                    bytes.limit(size - POSITION_SIZE);
                    bytes.put(data);
                    pages.put(this);
                    return true;
                }
            }
            return false;
        }

        public void write(long position, ByteBuffer data, PageMap pages)
        {
            ByteBuffer bytes = getByteBuffer();
            bytes.clear();
            bytes.position((int) (position - getPosition()));
            int size = bytes.getInt();
            bytes.limit(bytes.position() + size);
            bytes.put(data);
            pages.put(this);
        }

        public ByteBuffer getBlockRange(ByteBuffer bytes)
        {
            bytes.position(PAGE_HEADER_SIZE);
            bytes.limit(bytes.capacity());
            return bytes;
        }

        public int getOffset(long position)
        {
            return (int) (position - getPosition());
        }

        public synchronized boolean free(Allocator allocator, long address, PageMap pages)
        {
            if (this.allocator == allocator)
            {
                allocator.unwrite(address);
                ByteBuffer bytes = getBlockRange(getByteBuffer());
                if (seek(bytes, address))
                {
                    int offset = bytes.position();

                    int size = getSize(bytes);
                    if (size > 0)
                    {
                        size = -size;
                    }
                    bytes.putInt(offset, size);

                    pages.put(this);
                }
                return true;
            }
            return false;
        }

        public void relocate(DataPage to, PageMap pages)
        {
            setPosition(to.getPosition());
            ByteBuffer bytes = getByteBuffer();
            for (int i = 0; i < count; i++)
            {
                int offset = bytes.position();
                int length = bytes.getInt();
                long address = bytes.getLong();
                AddressPage addressPage = getPager().getAddressPage(address);
                addressPage.set(address, getPosition() + offset);
                pages.put(addressPage);
                bytes.position(bytes.position() + length - POSITION_SIZE);
            }
        }
    }

    private final static class JournalPage
    extends Page
    {
        private ByteBuffer bytes;

        private int offset;

        public JournalPage(Pager pager, long position)
        {
            super(pager, position);

            bytes = getByteBuffer();

            bytes.clear();
            bytes.getLong();
            bytes.putInt(-1);

            offset = bytes.position();
        }

        protected void initialize(ByteBuffer bytes)
        {
            bytes.putLong(0L);
            bytes.putInt(-1);
        }

        public boolean write(Operation operation, PageMap pages)
        {
            ByteBuffer bytes = getByteBuffer();
            if (operation.length() + NEXT_PAGE_SIZE < bytes.capacity() - offset)
            {
                bytes.position(offset);
                operation.write(bytes);
                pages.put(this);
                return true;
            }
            return false;
        }

        public void next(long position)
        {
            ByteBuffer bytes = getByteBuffer();
            bytes.position(offset);
            new NextOperation(position).write(bytes);
        }

        public long getJournalPosition()
        {
            return getPosition() + offset;
        }

        public void seek(long position)
        {

        }

        public Operation next()
        {
            return null;
        }

        public void putBack(Shift shift, PageMap pages)
        {
        }
    }

    private static final class ReservationPage
    extends Page
    {
        private int addressCount;

        public ReservationPage(Pager pager, long position)
        {
            super(pager, position);
        }

        private int getOffset(AddressPage addressPage, long address)
        {
            return (int) (address - addressPage.getPosition());
        }

        protected void initialize(ByteBuffer bytes)
        {
            bytes.putLong(0L);
            bytes.putInt(-1);
            bytes.putInt(0);
        }

        private int search(ByteBuffer bytes, int offset)
        {
            int low = 4;
            int high = addressCount + 4;
            int mid = 0;
            int cur = 0;
            while (low <= high)
            {
                mid = (low + high) / 2;
                cur = bytes.getInt(COUNT_SIZE * mid);
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

        public boolean reserve(int offset, PageMap pages)
        {
            ByteBuffer bytes = getByteBuffer();
            int index = search(bytes, offset);
            if (index < 0)
            {
                for (int i = addressCount; i > index; i--)
                {
                    bytes.putInt((i + 5) * COUNT_SIZE, bytes.getInt((i + 4) * COUNT_SIZE));
                }
                bytes.putInt(index, offset);
                addressCount++;
                bytes.putInt(CHECKSUM_SIZE + COUNT_SIZE, addressCount);
                pages.put(this);
                return true;
            }
            return false;
        }

        public void remove(int offset)
        {
            ByteBuffer bytes = getByteBuffer();
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

    private static final class PageReference
    extends WeakReference
    {
        private final Long position;

        public PageReference(Page page, ReferenceQueue queue)
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
        private final DataPage dataPage;

        private final int remaining;

        public Size(DataPage page)
        {
            this.dataPage = page;
            this.remaining = page.getRemaining();
        }

        public Size(DataPage page, int size)
        {
            this.dataPage = page;
            this.remaining = size;
        }

        public DataPage getDataPage()
        {
            return dataPage;
        }

        public int getRemaining()
        {
            return remaining;
        }
    }

    private final static class BySizePageMap
    {
        private final int alignment;

        private final Map mapOfSizeLists;

        public BySizePageMap(int alignment)
        {
            if (alignment < 0 || (alignment & (alignment - 1)) != 0)
            {
                throw new IllegalArgumentException("Not a power of 2: " + alignment);
            }
            this.mapOfSizeLists = new TreeMap();
            this.alignment = alignment;
        }

        public void add(DataPage page)
        {
            add(new Size(page, page.getRemaining()));
        }

        public synchronized void add(Size size)
        {
            // Maybe don't round down if exact.
            int aligned = ((size.getRemaining() | alignment - 1) + 1) - alignment;
            if (aligned != 0)
            {
                Integer key = new Integer(aligned);
                LinkedList listOfSizes = (LinkedList) mapOfSizeLists.get(key);
                if (listOfSizes == null)
                {
                    listOfSizes = new LinkedList();
                    mapOfSizeLists.put(key, listOfSizes);
                }
                listOfSizes.addLast(size);
            }
        }

        public synchronized Size bestFit(int blockSize)
        {
            int aligned = ((blockSize | alignment - 1) + 1); // Round up.
            Iterator entries = mapOfSizeLists.entrySet().iterator();
            while (entries.hasNext())
            {
                Map.Entry entry = (Map.Entry) entries.next();
                Integer candidateSize = (Integer) entry.getKey();
                if (aligned <= candidateSize.intValue())
                {
                    LinkedList listOfSizes = (LinkedList) entry.getValue();
                    if (listOfSizes.size() == 0)
                    {
                        entries.remove();
                        continue;
                    }
                    return (Size) listOfSizes.removeFirst();
                }
            }
            return null;
        }

        public synchronized void remove(DataPage dataPage)
        {
            int size = ((dataPage.getRemaining() | alignment - 1) + 1) - alignment;
            Integer key = new Integer(size);

            LinkedList listOfSizes = (LinkedList) mapOfSizeLists.get(key);

            Iterator pages = listOfSizes.iterator();
            while (pages.hasNext())
            {
                Size candidate = (Size) pages.next();
                if (candidate.getDataPage().getPosition() == dataPage.getPosition())
                {
                    pages.remove();
                    return;
                }
            }

            throw new RuntimeException("Unmatched address in by size map.");
        }
    }

    private final static class Pager
    {
        private final Checksum checksum;

        private final FileChannel fileChannel;

        private final int pageSize;

        private final Map mapOfPagesByPosition;

        private final Map mapOfEmptyPages;

        private final Set setOfWriters;

        private final ReferenceQueue queue;

        public final File file;

        public final Map mapOfStaticPages;

        private final int alignment;

        public final BySizePageMap mapOfPagesBySize;

        private final Map mapOfPointerPages;

        private final ByteBuffer journalBuffer;

        private int pointerPageCount;

        private final long firstPointer;

        private final ByteBuffer pointerPageCountBytes;

        public Pager(File file, FileChannel fileChannel, int pageSize, int alignment, Map mapOfStaticPages, ByteBuffer journalBuffer, long firstPointer, int pointerPageCount)
        {
            this.file = file;
            this.fileChannel = fileChannel;
            this.alignment = alignment;
            this.pageSize = pageSize;
            this.firstPointer = firstPointer;
            this.pointerPageCount = pointerPageCount;
            this.checksum = new Adler32();
            this.mapOfPagesByPosition = new HashMap();
            this.setOfWriters = new HashSet();
            this.journalBuffer = journalBuffer;
            this.mapOfPointerPages = new HashMap();
            this.mapOfPagesBySize = new BySizePageMap(alignment);
            this.mapOfStaticPages = mapOfStaticPages;
            this.pointerPageCountBytes = ByteBuffer.allocateDirect(COUNT_SIZE);
            this.mapOfEmptyPages = new TreeMap(new Comparator()
            {
                public int compare(Object leftObject, Object rightObject)
                {
                    return ((Long) rightObject).compareTo((Long) leftObject);
                }
            });
            this.queue = new ReferenceQueue();
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

        public synchronized void collect()
        {
            PageReference pageReference = null;
            while ((pageReference = (PageReference) queue.poll()) != null)
            {
                mapOfPagesByPosition.remove(pageReference.getPosition());
            }
        }

        public synchronized long reserve(AddressPage addressPage, PageMap pages)
        {
            long address = addressPage.reserve(pages);
            if (address == 0L)
            {
                mapOfPointerPages.remove(new Long(addressPage.getPosition()));
            }
            return address;
        }

        public synchronized AddressPage getAddressPageX(long position)
        {
            AddressPage addressPage = null;
            if (mapOfPointerPages.size() == 0)
            {
                Journal journal = new Journal(this, new PageMap(this, 16));
                long firstDataPage = 0L;
                DataPage fromDataPage = getDataPage(firstDataPage);
                DataPage toDataPage = getDataPage();
                Allocator allocator = fromDataPage.getAllocator();
                journal.relocate(allocator, fromDataPage, toDataPage);
            }
            if (position != 0L)
            {
                addressPage = (AddressPage) getAddressPage(position);
            }
            if (addressPage == null)
            {
                addressPage = (AddressPage) mapOfPointerPages.values().iterator().next();
            }
            return addressPage;
        }

        private long fromWilderness()
        {
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

                ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);

                bytes.getLong(); // Checksum.
                bytes.putInt(0); // Count of blocks.

                checksum(checksum, bytes);

                bytes.clear();
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

            synchronized (this)
            {
                DataPage dataPage = new DataPage(this, position);

                addPageByPosition(dataPage);

                return dataPage.getPosition();
            }
        }

        public long newTemporaryPage(Page page)
        {
            long position = newTemporaryPage();
            page.initialize(position);
            addPageByPosition(page);
            return position;
        }

        public synchronized long newTemporaryPage()
        {
            // This will be pointless if we are quick to rewind the
            // wilderness.
            Iterator entries = mapOfEmptyPages.entrySet().iterator();
            if (entries.hasNext())
            {
                Map.Entry entry = (Map.Entry) entries.next();
                Long position = (Long) entry.getKey();
                try
                {
                    if (position.longValue() == fileChannel.position() - pageSize)
                    {
                        entries.remove();
                        return ((Long) entry.getValue()).longValue();
                    }
                }
                catch (IOException e)
                {
                    throw new Danger("io.position", e);
                }
            }
            return fromWilderness();
        }

        public synchronized DataPage getJournalPage()
        {
            // This will be pointless if we are quick to rewind the
            // wilderness.
            Iterator entries = mapOfEmptyPages.entrySet().iterator();
            if (entries.hasNext())
            {
                Map.Entry entry = (Map.Entry) entries.next();
                Long position = (Long) entry.getKey();
                try
                {
                    if (position.longValue() == fileChannel.position() - pageSize)
                    {
                        entries.remove();
                        return (DataPage) entry.getValue();
                    }
                }
                catch (IOException e)
                {
                    throw new Danger("io.position", e);
                }
            }
            return null;
        }

        public DataPage getDataPage()
        {
            synchronized (mapOfEmptyPages)
            {
                Iterator pages = mapOfEmptyPages.values().iterator();
                if (pages.hasNext())
                {
                    DataPage dataPage = (DataPage) pages.next();
                    pages.remove();
                    return dataPage;
                }
            }
            return null;
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

        public synchronized AddressPage getAddressPage(long position)
        {
            position = (long) Math.floor(position - (position % pageSize));
            AddressPage addressPage = (AddressPage) getPageByPosition(position);
            if (addressPage == null)
            {
                addressPage = new AddressPage(this, position);
                addPageByPosition(addressPage);
            }
            return addressPage;
        }

        public synchronized DataPage getDataPage(long position)
        {
            position = (long) Math.floor(position - (position % pageSize));
            DataPage dataPage = (DataPage) getPageByPosition(position);
            if (dataPage == null)
            {
                dataPage = new DataPage(this, position);
                addPageByPosition(dataPage);
            }
            return dataPage;
        }

        public synchronized ReservationPage getReservationPage(long position)
        {
            position = (long) Math.floor(position - (position % pageSize));
            ReservationPage journalPage = (ReservationPage) getPageByPosition(position);
            if (journalPage == null)
            {
                journalPage = new ReservationPage(this, position);
                addPageByPosition(journalPage);
            }
            return journalPage;
        }

        public synchronized JournalPage getJournalPage(long position)
        {
            position = (long) Math.floor(position - (position % pageSize));
            JournalPage journalPage = (JournalPage) getPageByPosition(position);
            if (journalPage == null)
            {
                journalPage = new JournalPage(this, position);
                addPageByPosition(journalPage);
            }
            return journalPage;
        }

        public synchronized JournalPage newJournalPage()
        {
            return null;
        }

        public long getPosition(long address)
        {
            return (long) Math.floor(address / pageSize);
        }
    }

    public final static class PageMap
    {
        private final Pager pager;

        private final Map mapOfPages;

        private final Map mapOfByteBuffers;

        private final int capacity;

        public PageMap(Pager pager, int capacity)
        {
            this.pager = pager;
            this.mapOfPages = new HashMap();
            this.mapOfByteBuffers = new HashMap();
            this.capacity = capacity;
        }

        public void put(Page page)
        {
            Long key = new Long(page.getPosition());
            mapOfPages.put(key, page);
            mapOfByteBuffers.put(key, page);
            if (mapOfPages.size() > capacity)
            {
                flush();
            }
        }

        public void flush()
        {
            FileChannel fileChannel = pager.getFileChannel();
            Iterator pages = mapOfPages.values().iterator();
            while (pages.hasNext())
            {
                Page page = (Page) pages.next();
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
        public void commit(Pager pager, Journal journal, PageMap pages)
        {
        }

        public JournalPage getJournalPage(Pager pager, JournalPage journalPage)
        {
            return journalPage;
        }

        public boolean write(Pager pager, long destination, ByteBuffer data, PageMap pages)
        {
            return false;
        }

        public boolean unwrite(JournalPage journalPage, long destination)
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

        public void commit(Pager pager, Journal journal, PageMap pages)
        {
            DataPage dataPage = pager.getDataPage(page);
            dataPage.allocate(address, length, pages);
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

        public boolean write(Pager pager, long destination, ByteBuffer data, PageMap pages)
        {
            if (address == destination)
            {
                DataPage dataPage = pager.getDataPage(source);
                dataPage.write(source, data, pages);
                return true;
            }
            return false;
        }

        public boolean unwrite(JournalPage journalPage, long destination, PageMap pages)
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
        public void commit(Pager pager, Journal journal, PageMap pages)
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

        public void commit(Pager pager, Journal journal, PageMap pages)
        {
            AddressPage toAddressPage = pager.getAddressPage(destination);
            long toPosition = toAddressPage.dereference(destination);
            DataPage toDataPage = pager.getDataPage(toPosition);

            ByteBuffer fromBytes = journal.read(source);

            Allocator allocator = null;
            do
            {
                allocator = toDataPage.getAllocator();
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

        public void commit(Pager pager, Journal journal, PageMap pages)
        {
            AddressPage addressPage = pager.getAddressPage(address);
            long referenced = addressPage.dereference(address);
            DataPage dataPage = pager.getDataPage(referenced);
            Allocator allocator = null;
            do
            {
                allocator = dataPage.getAllocator();
            }
            while (!allocator.free(dataPage, address, pages));
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

        public ByteBuffer getByteBuffer(Pager pager, ByteBuffer bytes)
        {
            DataPage page = pager.getDataPage(position);
            return page.read(position, new JournalBlockReader());
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

        private final LinkedList listOfPages;

        private final PageMap pages;

        private final BySizePageMap mapOfPagesBySize;

        private long journalStart;

        private JournalPage journalPage;

        private final Map mapOfRelocations;

        private final Map mapOfVacuums;

        public Journal(Pager pager, PageMap pages)
        {
            this.journalPage = pager.newJournalPage();
            this.journalStart = journalPage.getJournalPosition();
            this.pager = pager;
            this.listOfPages = new LinkedList();
            this.pages = pages;
            this.mapOfPagesBySize = new BySizePageMap(pager.getAlignment());
            this.mapOfRelocations = new HashMap();
            this.mapOfVacuums = new HashMap();
        }

        public boolean free(DataPage dataPage, long address, PageMap pages)
        {
            return dataPage.free(this, address, pages);
        }

        public synchronized boolean write(DataPage dataPage, long address, ByteBuffer data, PageMap pages)
        {
            return dataPage.write(this, address, data, pages);
        }

        public void relocate(Allocator allocator, DataPage from, DataPage to)
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

        public long allocate(int blockSize)
        {
            int fullSize = blockSize + BLOCK_HEADER_SIZE;

            Size size = mapOfPagesBySize.bestFit(fullSize);
            if (size == null)
            {
                size = new Size(pager.getDataPage());
                listOfPages.add(size.getDataPage());
            }

            return size.getDataPage().allocate(blockSize, pages);
        }

        public synchronized void rewrite(long address, ByteBuffer data)
        {
            Long position = (Long) mapOfVacuums.get(new Long(pager.getPosition(address)));
            if (position != null)
            {
                JournalPage journalPage = pager.getJournalPage(position.longValue());
                journalPage.seek(position.longValue());
                Operation operation = null;
                do
                {
                    operation = journalPage.next();
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
                JournalPage journalPage = pager.getJournalPage(position.longValue());
                journalPage.seek(position.longValue());
                Operation operation = null;
                do
                {
                    operation = journalPage.next();
                    journalPage = operation.getJournalPage(pager, journalPage);
                }
                while (!operation.unwrite(journalPage, address));
            }
        }

        public void shift(long position, ByteBuffer bytes)
        {
            DataPage dataPage = pager.getDataPage(position);
            dataPage.write(position, bytes, pages);
        }

        public void write(long position, ByteBuffer bytes)
        {
            DataPage dataPage = pager.getDataPage(position);
            dataPage.write(position, bytes, pages);
        }

        public ByteBuffer read(long position)
        {
            DataPage dataPage = pager.getDataPage(position);
            return dataPage.read(position, new JournalBlockReader());
        }

        public void write(Operation operation)
        {
            if (!journalPage.write(operation, pages))
            {
                JournalPage nextJournalPage = pager.newJournalPage();
                nextJournalPage.next(nextJournalPage.getPosition());
                journalPage = nextJournalPage;
                write(operation);
            }
        }

        public long terminate()
        {
            Iterator relocations = mapOfRelocations.entrySet().iterator();
            while (relocations.hasNext())
            {
                Map.Entry entry = (Map.Entry) relocations.next();
                Long from = (Long) entry.getKey();
                Long to = (Long) entry.getValue();
                write(new Relocate(from.longValue(), to.longValue()));
            }
            return 0;
        }
    }

    private interface Writer
    {
        public void relocate(long from, long to);
    }

    private interface Allocator
    {
        public boolean write(DataPage dataPage, long address, ByteBuffer data, PageMap pages);

        public void rewrite(long address, ByteBuffer data);

        public boolean free(DataPage dataPage, long address, PageMap pages);

        public void unwrite(long address);
    }

    private final static class NullAllocator
    implements Allocator
    {
        public boolean write(DataPage dataPage, long address, ByteBuffer data, PageMap pages)
        {
            return dataPage.write(this, address, data, pages);
        }

        public boolean free(DataPage dataPage, long address, PageMap pages)
        {
            return dataPage.free(this, address, pages);
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

        private final Journal journal;

        private final BySizePageMap mapOfPagesBySize;

        private final LinkedList listOfPages;

        private final Map mapOfAddresses;

        private long lastPointerPage;

        private final PageMap pages;

        public Mutator(Pager pager, Journal journal, PageMap pages)
        {
            this.pager = pager;
            this.journal = journal;
            this.mapOfPagesBySize = new BySizePageMap(pager.getAlignment());
            this.listOfPages = new LinkedList();
            this.pages = pages;
            this.mapOfAddresses = new HashMap();
        }

        public long allocate(int blockSize)
        {
            int fullSize = blockSize + BLOCK_HEADER_SIZE;

            int pageSize = pager.getPageSize();
            if (fullSize + PAGE_HEADER_SIZE > pageSize)
            {
                // Recurse.
            }

            Size size = null;
            DataPage dataPage = null;
            do
            {
                size = mapOfPagesBySize.bestFit(fullSize);
                if (size == null)
                {
                    size = pager.mapOfPagesBySize.bestFit(fullSize);
                    if (size == null)
                    {
                        size = new Size(pager.getDataPage());
                    }
                    listOfPages.add(size.getDataPage());
                }
                dataPage = size.getDataPage();
            }
            while (!dataPage.setAllocator(journal, false));

            int remaining = size.getRemaining() - fullSize;
            mapOfPagesBySize.add(new Size(dataPage, remaining));

            // Here is where I left off. The question is how do I manage the
            // pointer pages? Do the pointer pages create pointer objects? Do
            // I write out the entire pointer object or just the pointer?
            AddressPage addressPage = null;
            long address = 0L;
            do
            {
                addressPage = pager.getAddressPageX(lastPointerPage);
                address = addressPage.reserve(pages);
                if (address == 0L)
                {
                    address = pager.reserve(addressPage, pages);
                }
            }
            while (address == 0L);

            long position = journal.allocate(fullSize);

            mapOfAddresses.put(new Long(address), new Long(position));

            journal.write(new Allocate(address, dataPage.getPosition(), position, fullSize));

            /*
             * When pointers are written out, only the 8 bytes are written
             * out. But the page will have the 8 bytes with a value, will not
             * be reused. Pointer is written out as part of Allocate. Need a
             * place to write. If the page has broken chunks, we need to close
             * them up, scan through page looking for broken chunks.
             */
            return address;
        }

        public void commit()
        {
        }

        public ByteBuffer read(long address)
        {
            return null;
        }

        public void write(long address, ByteBuffer bytes)
        {
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
            ByteBuffer bytes = mutator.read(address);
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

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */