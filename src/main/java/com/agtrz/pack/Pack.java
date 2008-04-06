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
import java.util.SortedMap;
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

    // FIXME Reorder these numbers.
    private final static short WRITE = 2;

    private final static short FREE = 3;

    private final static short NEXT_PAGE = 4;

    private final static short COMMIT = 5;

    private final static short ADD_VACUUM = 6;

    private final static short VACUUM = 7;

    private final static short ADD_MOVE = 8;
    
    private final static short SHIFT_MOVE = 9;
    
    private final static short TERMINATE = 10;

    private final static int NEXT_PAGE_SIZE = FLAG_SIZE + ADDRESS_SIZE;

    private final static int ADDRESS_PAGE_HEADER_SIZE = CHECKSUM_SIZE + POSITION_SIZE;

    private final static int RESERVATION_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    private final static int JOURNAL_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    private final Pager pager;
    
    /**
     * Create a new pack from the specified pager.
     * <p>
     * One of these days, I'll have to determine if the pager class contents
     * could be within the pack.
     */
    public Pack(Pager pager)
    {
        this.pager = pager;
    }

    /**
     * Create an object that can inspect and alter the contents of this pack.
     * 
     * @return A new {@link Pack.Mutator}.
     */
    public Mutator mutate()
    {
        final PageRecorder pageRecorder = new PageRecorder();
        final MoveList listOfMoves = new MoveList(pageRecorder, pager.getMoveList());
        return listOfMoves.mutate(new Returnable<Mutator>()
        {
            public Mutator run()
            {
                MoveNode moveNode = new MoveNode(new Move(0, 0));
                DirtyPageMap dirtyPages = new DirtyPageMap(pager, 16);
                Journal journal = new Journal(pager, pageRecorder, moveNode, dirtyPages);
                return new Mutator(pager, listOfMoves, pageRecorder, journal, moveNode, dirtyPages);
            }
        });
    }

    /**
     * Soft close of the pack will wait until all mutators commit or rollback
     * and then compact the pack before closing the file.
     * <p>
     * FIXME This is an incomplete implementation of close.
     */
    public void close()
    {
        pager.getCompactLock().writeLock().lock();
        try
        {
            try
            {
                pager.getFileChannel().close();
            }
            catch (IOException e)
            {
                throw new Danger("io.close", e);
            }
        }
        finally
        {
            pager.getCompactLock().writeLock().unlock();
        }
    }

    /**
     * Create a new file channel from the specified file.
     * 
     * @param file The file to read and write.
     * @return A file channel that reads and writes the specified file.
     */
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

            Pager pager = new Pager(file, fileChannel, pageSize, alignment, mapOfStaticPages, internalJournalCount, pointerPageCount);

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

            return new Pack(new Pager(file, fileChannel, pageSize, alignment, mapOfStaticPages, internalJournalCount, pointerPageCount));
        }
    }
    
    public final static class Opener
    {
        public Pack open(File file)
        {
            return null;
        }
    }

    private static final class ShiftMove extends Operation
    {
        @Override
        public void commit(Player player)
        {
            player.getMoveList().removeFirst();
        }

        @Override
        public int length()
        {
            return FLAG_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(SHIFT_MOVE);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
        }
    }
    
    private static final class Move
    {
        private final long from;
        
        private final long to;
        
        public Move(long from, long to)
        {
            assert from != to || from == 0;
            assert (from == 0 && to == 0) || !(from == 0 || to == 0);

            this.from = from;
            this.to = to;
        }
        
        public long getFrom()
        {
            return from;
        }
        
        public long getTo()
        {
            return to;
        }
    }

    private static final class MoveLatch
    {
        private Move move;

        private Lock lock;
        
        private MoveLatch next;

        public MoveLatch(Move move, MoveLatch next)
        {
            Lock lock = new ReentrantLock();
            lock.lock();
            
            this.move = move;
            this.lock = lock;
            this.next = next;
        }

        public Lock getLock()
        {
            return lock;
        }

        public Move getMove()
        {
            return move;
        }
        
        public MoveLatch getNext()
        {
            return next;
        }

        public void extend(MoveLatch next)
        {
            assert this.next == null;

            this.next = next;
        }

        public MoveLatch getLast()
        {
            MoveLatch iterator = this;
            while (iterator.next == null)
            {
                iterator = iterator.next;
            }
            return iterator;
        }
    }
    
    private static final class MoveNode
    {
        private final Move move;
        
        private MoveNode next;
        
        public MoveNode(Move move)
        {
            this.move = move;
        }
        
        public Move getMove()
        {
            return move;
        }
        
        public MoveNode getNext()
        {
            return next;
        }
        
        public MoveNode getLast()
        {
            MoveNode iterator = this;
            while (iterator.next == null)
            {
                iterator = iterator.next;
            }
            return iterator;
        }
        
        public MoveNode extend(Move move)
        {
            assert next == null;
            
            return next = new MoveNode(move);
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
        public Position getPosition();

        public void create(Position page, DirtyPageMap pages);

        public void load(Position page);

        public boolean isInterim();
    }

    private static final class AddressPage
    implements Page
    {
        private final Lock lock;

        private Position position;

        private long reservations;
        
        public AddressPage()
        {
            this.lock = new ReentrantLock();
        }

        public void load(Position position)
        {
            position.setPage(this);
            this.position = position;
            
            ByteBuffer bytes = position.getByteBuffer();

            bytes.clear();
            
            bytes.getLong();
            reservations = bytes.getLong();
        }

        public void create(Position position, DirtyPageMap pages)
        {
            ByteBuffer bytes = position.getByteBuffer();

            int capacity = bytes.capacity() / ADDRESS_SIZE;
            for (int i = 0; i < capacity; i++)
            {
                bytes.putLong(i * ADDRESS_SIZE, 0L);
            }

            pages.add(position);

            this.position = position;
        }

        public Lock getLock()
        {
            return lock;
        }

        public Position getPosition()
        {
            return position;
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
                offset = (int) firstPointer;
            }
            return offset + ADDRESS_PAGE_HEADER_SIZE;
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
                page = (ReservationPage) pager.newSystemPage(new ReservationPage(), dirtyPages);
                reservations = page.getPosition().getValue();
                
                ByteBuffer bytes = getPosition().getByteBuffer();
                
                bytes.putLong(CHECKSUM_SIZE, 0L);

                dirtyPages.add(getPosition());
            }
            else
            {
                page = (ReservationPage) pager.getPage(reservations, new ReservationPage());
            }

            return page;
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
            synchronized (getPosition())
            {
                // Get the reservation page. Note that the page type holds a
                // hard reference to the page. 

                ReservationPage reserver = getReservationPage(dirtyPages);

                // Get the page buffer.
                
                ByteBuffer bytes = position.getByteBuffer();

                // Iterate the page buffer looking for a zeroed address that has
                // not been reserved, reserving it and returning it if found.
                
                long position = getPosition().getValue();
                for (int i = getStartOffset(); i < bytes.capacity(); i += ADDRESS_SIZE)
                {
                    if (bytes.getLong(i) == 0L && reserver.reserve(position, position + i, dirtyPages))
                    {
                        return position + i;
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

        public void set(long address, long value, DirtyPageMap dirtyPages)
        {
            // Out here to hold onto reference until it is added to dirty pages.
            ByteBuffer bytes = null;
            
            synchronized (getPosition())
            {
                bytes = position.getByteBuffer();
                bytes.putLong((int) (address - position.getValue()), value);
                
                if (reservations != 0L)
                {
                    ReservationPage reservationPage = getPosition().getPager().getPage(reservations, new ReservationPage());
                    reservationPage.remove(getPosition().getValue(), address, dirtyPages);
                    if (reservationPage.getReservedCount() == 0)
                    {
                        reservations = 0L;
                        bytes.putLong(CHECKSUM_SIZE, 0L);
                    }
                }
            }
            
            dirtyPages.add(getPosition());
        }
    }

    private static class RelocatablePage
    implements Page
    {
        private Position position;
        
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

        public Position getPosition()
        {
            return position;
        }

        public boolean isInterim()
        {
            return false;
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

    /**
     * A page strategy that manages the page a list of data blocks.
     * <p>
     * Vacuum must work in two stages. The page is mirrored. The blocks that
     * are preceded by one or more freed blocks are copied into interim pages.
     * Then during journal play back, the compacting is performed by copying
     * the mirrored blocks into place over the freed blocks.
     * <p>
     * Once a page is mirrored, no other mutator can write to that page, since
     * that would put it out of sync with the mirroring of the page. If we
     * were to mirror a page and then another mutator updated a block in the
     * page, if the blocks is preceded by one or more freed blocks, then that
     * block would be reverted when we compact the page from the mirror.
     * <p>
     * Initially, you thought that a strategy was have the writing mutator
     * also update the mirror. This caused a lot of confusion, since now the
     * journal was changing after the switch to play back. How does one
     * mutator write to another mutator's journal? Which mutator commits that
     * change? This raised so many questions, I can't remember them all.
     * <p>
     * The mirrored property is checked before an mutator writes or frees a
     * block. If it is true, indicating that the page is mirrored but not
     * compacted, then the operation will block until the compacting makes the
     * vacuum complete.
     * <p>
     * Vacuums occur before all other play back operations. During play back
     * after a hard shutdown, we run the vacuums before all other operations.
     * We run the vacuums of each journal, then we run the remainder of each
     * journal.
     * <p>
     * Every once and a while, you forget and worry about deadlock. You're
     * afraid that one thread holding on a mirrored data page will attempt to
     * write to a mirrored data page of anther thread while that thread is
     * trying to write a mirrored data page held the this thread. This cannot
     * happen, of course, because vacuums happen before write or free
     * operations.
     * <p>
     * You cannot deadlock by mirroring, because only one mutator at a time
     * will ever vacuum a data page, because only one mutator at a time can
     * use a data page for block allocation.
     */
    private static final class DataPage
    extends RelocatablePage
    {
        private int remaining;

        private int count;

        private boolean system;
        
        /**
         * True if the page is in the midst of a vacuum and should not
         * be written to.
         */
        private boolean mirrored;
        
        public DataPage()
        {
            mirrored = false;
        }
        
        public void create(Position position, DirtyPageMap dirtyPages)
        {
            super.create(position, dirtyPages);
            
            this.count = 0;
            this.remaining = position.getPager().getPageSize() - DATA_PAGE_HEADER_SIZE;
            
            dirtyPages.add(position);
        }

        public void load(Position position)
        {    
            super.load(position);

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
                    bytes.position(bytes.position() - COUNT_SIZE + ADDRESS_SIZE);
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

        public boolean mirror(Pager pager, Journal journal, DirtyPageMap dirtyPages)
        {
            DataPage vacuumPage = null;
            synchronized (getPosition())
            {
                assert !mirrored;

                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                int block = 0;
                boolean deleted = false;
                while (block != count)
                {
                    int size = getSize(bytes);
                    if (size < 0)
                    {
                        bytes.position(bytes.position() + Math.abs(size));
                        deleted = true;
                    }
                    else
                    {
                        block++;
                        if (size > remaining)
                        {
                            throw new IllegalStateException();
                        }
                        if (deleted)
                        {
                            vacuumPage = pager.newSystemPage(new DataPage(), dirtyPages);

                            int length = Math.abs(size);
                            long address = bytes.getLong(bytes.position());

                            ByteBuffer data = ByteBuffer.allocateDirect(Math.abs(size));
                            data.put(bytes);
    
                            long position = vacuumPage.allocate(address, length, dirtyPages);
                            vacuumPage.write(position, address, data, dirtyPages);
                        }
                    }
                }
                if (vacuumPage != null)
                {
                    mirrored = true;
                    journal.write(new AddVacuum(vacuumPage.getPosition().getValue()));
                }
                return mirrored;
            }
        }

        public long allocate(long address, int length, DirtyPageMap dirtyPages)
        {
            long position = 0L;
            synchronized (getPosition())
            {
                assert ! mirrored;

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
                    remaining -= length;

                    bytes.clear();
                    bytes.getLong();
                    bytes.putInt(count);
                }
            }

            dirtyPages.add(getPosition());

            return position;
        }

        // FIXME This is the method I should use, right?
        public boolean write(long address, ByteBuffer data, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
                // FIXME I forgot what this was about.
                while (mirrored)
                {
                    try
                    {
                        getPosition().wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                if (seek(bytes, address))
                {
                    int size = getSize(bytes);
                    bytes.putLong(address);
                    bytes.limit(size - POSITION_SIZE);
                    bytes.put(data);
                    pages.add(getPosition());
                    return true;
                }
                return false;
            }
        }

        public void write(long position, long address, ByteBuffer data, DirtyPageMap dirtyPages)
        {
            // FIXME Seek.
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
                dirtyPages.add(getPosition());
            }
        }

        /**
         * Return the byte buffer associated with this data page with the
         * position and limit set to the range of bytes that contain blocks.
         *
         * @return The byte buffer limited to the block range.
         */
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

        public boolean free(long address, DirtyPageMap pages)
        {
            synchronized (getPosition())
            {
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

                    pages.add(getPosition());
                }
                return true;
            }
        }

        public void relocate(Position to, DirtyPageMap dirtyPages)
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

                    AddressPage addressPage = (AddressPage) getPosition().getPager().getPage(address, new AddressPage());

                    addressPage.set(address, getPosition().getValue() + offset, dirtyPages);

                    dirtyPages.add(addressPage.getPosition());

                    bytes.position(bytes.position() + length - POSITION_SIZE);
                }
            }
        }

        public void compact()
        {
            throw new UnsupportedOperationException();
        }
        
        public void commit(long address, ByteBuffer block, DirtyPageMap dirtyPages)
        {
            synchronized (getPosition())
            {
                AddressPage addressPage = getPosition().getPager().getPage(address, new AddressPage());
                synchronized (addressPage.getPosition())
                {
                    ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                    if (seek(bytes, address))
                    {
                        int size = getSize(bytes);
                        
                        assert size == block.remaining() + BLOCK_HEADER_SIZE;
                        
                        assert bytes.getLong() == address;
                        
                        bytes.put(block);
                    }
                    else
                    {
                        assert block.remaining() + BLOCK_HEADER_SIZE < bytes.remaining();
                        
                        int offset = bytes.position();

                        bytes.putInt(block.remaining() + BLOCK_HEADER_SIZE);
                        bytes.putLong(address);
                        bytes.put(block);
                        
                        count++;
                        
                        bytes.putInt(POSITION_SIZE, count);
                        
                        addressPage.set(address, getPosition().getValue() + offset, dirtyPages);
                    }
                }
            }
            // FIXME No hard reference to bytes.
            dirtyPages.add(getPosition());
        }

        public void commit(DataPage dataPage, DirtyPageMap dirtyPages)
        {
            // FIXME Locking a lot. Going to deadlock?
            synchronized (getPosition())
            {
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                int i = 0;
                while (i < count)
                {
                    int size = getSize(bytes);
                    if (size > 0)
                    {
                        long address = bytes.getLong();

                        bytes.limit(bytes.position() + (size - BLOCK_HEADER_SIZE));
                        dataPage.commit(address, bytes.slice(), dirtyPages);

                        bytes.limit(bytes.capacity());
                    }
                    i++;
                }
            }
        }

        public int getSize(MovablePosition position, long address)
        {
            synchronized (getPosition())
            {
                ByteBuffer bytes = getBlockRange(getPosition().getByteBuffer());
                if (seek(bytes, address))
                {
                    return Math.abs(getSize(bytes));
                }
            }
            throw new IllegalStateException();
        }
    }

    private final static class JournalPage
    extends RelocatablePage
    {
        private int offset;

        public void create(Position position, DirtyPageMap dirtyPages)
        {
            super.create(position, dirtyPages);

            ByteBuffer bytes = getPosition().getByteBuffer();
            
            bytes.clear();
            bytes.getLong();
            bytes.putInt(-1);

            getPosition().setPage(this);
            
            this.offset = JOURNAL_PAGE_HEADER_SIZE;
        }

        public void load(Position position)
        {
            super.load(position);
            
            this.offset = JOURNAL_PAGE_HEADER_SIZE;
        }

        public boolean isInterim()
        {
            return true;
        }
        
        private ByteBuffer getByteBuffer()
        {
            ByteBuffer bytes = getPosition().getByteBuffer();
            
            bytes.clear();
            bytes.position(offset);

            return bytes;
        }

        public boolean write(Operation operation, DirtyPageMap dirtyPages)
        {
            synchronized (getPosition())
            {
                ByteBuffer bytes = getByteBuffer();

                if (operation.length() + NEXT_PAGE_SIZE < bytes.remaining())
                {
                    operation.write(bytes);
                    offset = bytes.position();
                    dirtyPages.add(getPosition());
                    return true;
                }
                
                return false;
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
            synchronized (getPosition())
            {
                this.offset = (int) (position - getPosition().getValue());
            }
        }

        private Operation newOperation(short type)
        {
            switch (type)
            {
                case WRITE:
                    return new Write();
                case FREE:
                    return new Free();
                case NEXT_PAGE:
                    return new NextOperation();
                case COMMIT:
                    return new Commit();
                case ADD_VACUUM:
                    return new AddVacuum();
                case VACUUM:
                    return new Vacuum();
                case ADD_MOVE: 
                    return new AddMove();
                case SHIFT_MOVE:
                    return new ShiftMove();
                case TERMINATE:
                    return new Terminate();
            }
            throw new IllegalStateException();
        }
        
        public Operation next()
        {
            ByteBuffer bytes = getByteBuffer();

            Operation operation = newOperation(bytes.getShort());
            operation.read(bytes);
            
            offset = bytes.position();
            
            return operation;
        }
    }

    private static final class ReservationPage
    extends RelocatablePage
    {
        private int reserved;

        public void create(Position page, DirtyPageMap dirtyPages)
        {
            super.create(page, dirtyPages);

            ByteBuffer bytes = page.getByteBuffer();

            bytes.clear();

            bytes.putLong(0L);
            bytes.putInt(-1);
            
            while (bytes.remaining() != 0)
            {
                bytes.put((byte) 0);
            }

            dirtyPages.add(page);
        }

        public void load(Position page)
        {
            super.load(page);

            ByteBuffer bytes = page.getByteBuffer();
            
            bytes.getLong();
            bytes.getInt();
            
            while (bytes.remaining() != 0)
            {
                if (bytes.get() == 1)
                {
                    reserved++;
                }
            }
        }

        public boolean isInterim()
        {
            return true;
        }

        public int getReservedCount()
        {
            return reserved;
        }

        public boolean reserve(long position, long address, DirtyPageMap dirtyPages)
        {
            int offset = getOffset(position, address);
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
                if (bytes.get(offset) == 0) 
                {
                    bytes.put(offset, (byte) 1);
                    dirtyPages.add(getPosition());
                    reserved++;
                    return true;
                }
                assert bytes.get(offset) == 1;
                return false;
            }
        }

        private int getOffset(long position, long address)
        {
            int offset = (int) ((address - position - ADDRESS_PAGE_HEADER_SIZE) / POSITION_SIZE);
            return RESERVATION_PAGE_HEADER_SIZE + offset;
        }

        public void remove(long position, long address, DirtyPageMap dirtyPages)
        {
            int offset = getOffset(position, address);
            synchronized (getPosition())
            {
                ByteBuffer bytes = getPosition().getByteBuffer();
                if (bytes.get(offset) == 1)
                {
                    bytes.put(offset, (byte) 0);
                    dirtyPages.add(getPosition());
                    reserved--;
                }
                assert bytes.get(offset) == 0;
            }
        }
    }

    private static class MovablePosition
    {
        protected final MoveNode moveNode;
        
        protected final long position;
        
        public MovablePosition(MoveNode moveNode, long position)
        {
            this.moveNode = moveNode;
            this.position = position;
        }
        
        public long getValue(Pager pager)
        {
            return pager.adjust(moveNode, position, 0);
        }
    }

    private static final class SkippingMovablePosition
    extends MovablePosition
    {
        public SkippingMovablePosition(MoveNode moveNode, long position)
        {
            super(moveNode, position);
        }
        
        public long getValue(Pager pager)
        {
            return pager.adjust(moveNode, position, 1);
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

    private final static class PageRecorder implements MoveRecorder
    {
        private final Set<Long> setOfInterimPages;
        
        private final Set<Long> setOfAllocationPages;
        
        public PageRecorder()
        {
            this.setOfInterimPages = new HashSet<Long>();
            this.setOfAllocationPages = new HashSet<Long>();
        }
        
        public Set<Long> getInterimPageSet()
        {
            return setOfInterimPages;
        }
        
        public Set<Long> getAllocationPageSet()
        {
            return setOfAllocationPages;
        }
        
        public boolean contains(long position)
        {
            return setOfInterimPages.contains(position)
                || setOfAllocationPages.contains(position);
        }

        public void record(Move move)
        {
            if (setOfInterimPages.remove(move.getFrom()))
            {
                setOfInterimPages.add(move.getTo());
            }
            if (setOfAllocationPages.remove(move.getFrom()))
            {
                setOfAllocationPages.add(move.getTo());
            }
        }
    }
    
    private interface MoveRecorder
    {
        public boolean contains(long position);

        public void record(Move move);
    }

    private final static class BySizeTable
    {
        private final int alignment;

        private final List<LinkedList<Long>> listOfListsOfSizes;

        public BySizeTable(int pageSize, int alignment)
        {
            assert pageSize % alignment == 0;

            ArrayList<LinkedList<Long>> listOfListsOfSizes = new ArrayList<LinkedList<Long>>(pageSize / alignment);

            for (int i = 0; i < pageSize / alignment; i++)
            {
                listOfListsOfSizes.add(new LinkedList<Long>());
            }

            this.alignment = alignment;
            this.listOfListsOfSizes = listOfListsOfSizes;
        }
        
        public int getSize()
        {
            int size = 0;
            for (List<Long> listOfSizes : listOfListsOfSizes)
            {
                size += listOfSizes.size();
            }
            return size;
        }

        public synchronized void add(DataPage dataPage)
        {
            // Maybe don't round down if exact.
            int aligned = ((dataPage.getRemaining() | alignment - 1) + 1) - alignment;
            if (aligned != 0)
            {
                listOfListsOfSizes.get(aligned / alignment).addFirst(dataPage.getPosition().getValue());
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
        public synchronized long bestFit(int blockSize)
        {
            long bestFit = 0L;
            int aligned = ((blockSize | alignment - 1) + 1); // Round up.
            if (aligned != 0)
            {
                for (int i = aligned / alignment; bestFit == 0L && i < listOfListsOfSizes.size(); i++)
                {
                    if (!listOfListsOfSizes.get(i).isEmpty())
                    {
                        bestFit = listOfListsOfSizes.get(i).removeFirst();
                    }
                }
            }
            return bestFit;
        }

        public synchronized void join(BySizeTable pagesBySize, Set<Long> setOfDataPages, Map<Long, MovablePosition> mapOfPages, MoveNode moveNode)
        {
            for (int i = 0; i < pagesBySize.listOfListsOfSizes.size(); i++)
            {
                List<Long> listOfSizes = pagesBySize.listOfListsOfSizes.get(i);
                for (long size: listOfSizes)
                {
                    long found = bestFit((i + 1) * alignment);
                    if (found != 0L)
                    {
                        setOfDataPages.add(found);
                        mapOfPages.put(size, new MovablePosition(moveNode, found));
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

        private MoveLatch headOfMoves;
        
        public MoveList()
        {
            this.recorder = new NullMoveRecorder();
            this.headOfMoves = new MoveLatch(new Move(0, 0), null);
            this.readWriteLock = new ReentrantReadWriteLock();
        }

        public MoveList(MoveRecorder recorder, MoveList listOfMoves)
        {
            this.recorder = recorder;
            this.headOfMoves = listOfMoves.headOfMoves;
            this.readWriteLock = listOfMoves.readWriteLock;
        }
        
        public void add(MoveLatch move)
        {
            readWriteLock.writeLock().lock();
            try
            {
                MoveLatch iterator = headOfMoves;
                while (iterator.getNext() != null)
                {
                    iterator = iterator.getNext();
                }
                iterator.extend(move);
                
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
                        if (recorder.contains(headOfMoves.getMove().getFrom()))
                        {
                            headOfMoves.getLock().lock();
                            headOfMoves.getLock().unlock();
                            recorder.record(headOfMoves.getMove());
                        }
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
                        if (recorder.contains(headOfMoves.getMove().getFrom()))
                        {
                            headOfMoves.getLock().lock();
                            headOfMoves.getLock().unlock();
                            recorder.record(headOfMoves.getMove());
                        }
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
         * A read/write lock that coordinates rewind of area boundaries and the
         * wilderness. 
         */
        private final ReadWriteLock compactLock;
        
        /**
         * A read/write lock that protects the end of the move list.
         */
        private final ReadWriteLock moveListLock;
        
        /**
         * A lock to ensure that only one mutator at a time is moving pages in
         * the interim page area.
         */
        private final Lock expandLock;

        private final ReferenceQueue<Position> queue;

        public final File file;

        public final Map<URI, Long> mapOfStaticPages;

        private final int alignment;

        public final BySizeTable pagesBySize;

        private final SortedMap<Long, AddressPage> mapOfPointerPages;

        private final SortedSet<Long> setOfFreeUserPages;

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
        private final SortedSet<Long> setOfFreeInterimPages;

        private int pointerPageCount;

        private long firstSystemPage;
            
        private final MoveList listOfMoves;
        
        private final PositionSet setOfJournalHeaders;
        
        public Pager(File file, FileChannel fileChannel, int pageSize, int alignment, Map<URI, Long> mapOfStaticPages, int internalJournalCount, int pointerPageCount)
        {
            this.file = file;
            this.fileChannel = fileChannel;
            this.alignment = alignment;
            this.pageSize = pageSize;
            this.pointerPageCount = pointerPageCount;
            this.checksum = new Adler32();
            this.mapOfPagesByPosition = new HashMap<Long, PageReference>();
            this.mapOfPointerPages = new TreeMap<Long, AddressPage>();
            this.pagesBySize = new BySizeTable(pageSize, alignment);
            this.mapOfStaticPages = mapOfStaticPages;
            this.setOfFreeUserPages = new TreeSet<Long>();
            this.setOfFreeInterimPages = new TreeSet<Long>(new Reverse<Long>());
            this.queue = new ReferenceQueue<Position>();
            this.moveListLock = new ReentrantReadWriteLock();
            this.compactLock = new ReentrantReadWriteLock();
            this.expandLock = new ReentrantLock();
            this.listOfMoves = new MoveList();
            this.setOfJournalHeaders = new PositionSet(FILE_HEADER_SIZE, internalJournalCount);
        }
        
        public long getFirstPointer()
        {
            return FILE_HEADER_SIZE + (setOfJournalHeaders.getCapacity() * POSITION_SIZE);
        }
        
        public Lock getExpandLock()
        {
            return expandLock;
        }
        
        public PositionSet getJournalHeaderSet()
        {
            return setOfJournalHeaders;
        }
        
        public ReadWriteLock getCompactLock()
        {
            return compactLock;
        }
        
        public ReadWriteLock getMoveListLock()
        {
            return moveListLock;
        }
        
        public synchronized long getFirstInterimPage()
        {
            return firstSystemPage;
        }
        
        public synchronized void setFirstSystemPage(long firstSystemPage)
        {
            this.firstSystemPage = firstSystemPage;
        }

        public synchronized void setFirstInterimPage(long firstInterimPage)
        {
            this.firstSystemPage = firstInterimPage;
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

            long firstPointerPage = getFirstPointer();
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
                if (firstPointerPage < getFirstPointer())
                {
                    offset = (int) getFirstPointer() / POSITION_SIZE;
                }
                while (offset < bytes.capacity() / POSITION_SIZE)
                {
                    if (bytes.getLong(offset) == 0L)
                    {   
                        AddressPage addressPage = (AddressPage) getPage(firstPointerPage, new AddressPage());
                        mapOfPointerPages.put(firstPointerPage, addressPage);
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

        public void newUserDataPages(Set<Long> setOfPages, Set<Long> setOfDataPages, Map<Long, MovablePosition> mapOfPages, MoveNode moveNode)
        {
            synchronized (setOfFreeUserPages)
            {
                while (setOfFreeUserPages.size() != 0 && setOfPages.size() != 0)
                {
                    Iterator<Long> pages = setOfPages.iterator();
                    Iterator<Long> freeUserPages = setOfFreeUserPages.iterator();
                    long position = freeUserPages.next();
                    setOfDataPages.add(position);
                    mapOfPages.put(pages.next(), new MovablePosition(moveNode, position));
                    pages.remove();
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

        public synchronized long reserve(AddressPage addressPage, DirtyPageMap pages)
        {
            long address = addressPage.reserve(pages);
            if (address == 0L)
            {
                mapOfPointerPages.remove(addressPage.getPosition().getValue());
            }
            return address;
        }

        public synchronized AddressPage getAddressPage(long position, DirtyPageMap dirtyPages)
        {
            AddressPage addressPage = null;
            if (mapOfPointerPages.size() == 0)
            {
//                Journal journal = new Journal(this, null, null, new DirtyPageMap(this, 16));
//                long firstDataPage = 0L;
//                DataPage fromDataPage = (DataPage) getPage(firstDataPage, new DataPage());
//                DataPage toDataPage = newDataPage(dirtyPages);
//                journal.relocate(fromDataPage, toDataPage);
            }
            if (position != 0L)
            {
                addressPage = (AddressPage) getPage(position, new AddressPage());
            }
            if (addressPage == null)
            {
                addressPage = mapOfPointerPages.get(mapOfPointerPages.firstKey());
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

        public DataPage newDataPage(DirtyPageMap pages)
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
            Position position = null;
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
                            position = getPageByPosition(firstSystemPage);
                            if (position == null)
                            {
                                position = new Position(this, firstSystemPage);
                                addPageByPosition(position);
                            }
                            dataPage.create(position, pages);
                        }
                    }
                }
                if (position == null)
                {
                    synchronized (mapOfPagesByPosition)
                    {
                        position = getPageByPosition(firstSystemPage);
                        assert position != null;
                    }
                    // Move.
                }
                firstSystemPage += getPageSize();
            }
            return dataPage;
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
        
        @SuppressWarnings("unchecked")
        public <P extends Page> P setPage(long value, P page, DirtyPageMap dirtyPages)
        {
            value = (long) Math.floor(value - (value % pageSize));
            Position position = new Position(this, value);
            page.create(position, dirtyPages);

            synchronized (mapOfPagesByPosition)
            {
                assert getPageByPosition(value) == null;
                addPageByPosition(position);
            }

            return (P) position.getPage();
        }

        @SuppressWarnings("unchecked")
        public <P extends Page> P getPage(long value, P page)
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
            }
            synchronized (position)
            {
                if (!position.getPage().getClass().equals(page.getClass()))
                {
                    page.load(position);
                }
            }
            return (P) position.getPage();
        }

        private long popFreeInterimPage()
        {
            long position = 0L;
            synchronized (setOfFreeInterimPages)
            {
                if (setOfFreeInterimPages.size() > 0)
                {
                    position = setOfFreeInterimPages.last();
                    setOfFreeInterimPages.remove(position);
                }
            }
            return position;
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
        public <T extends Page> T newSystemPage(T page, DirtyPageMap dirtyPages)
        {
            // FIXME Rename newInterimPage.

            // We pull from the end of the interim space to take pressure of of
            // the durable pages, which are more than likely multiply in number
            // and move interim pages out of the way. We could change the order
            // of the interim page set, so that we choose free interim pages
            // from the front of the interim page space, if we want to rewind
            // the interim page space and shrink the file more frequently.

            long value = popFreeInterimPage();

            // If we do not have a free interim page available, we will obtain
            // create one out of the wilderness.

            if (value == 0L)
            {
                value = fromWilderness();
            }

            Position position = new Position(this, value);

            page.create(position, dirtyPages);

            synchronized (mapOfPagesByPosition)
            {
                addPageByPosition(position);
            }

            return page;
        }

        public long getPosition(long address)
        {
            return (long) Math.floor(address / pageSize);
        }
        
        /**
         * Return an interim page for use as a move destination.
         * <p>
         * Question: How do we ensure that free interim pages do not slip into
         * the user data page section? That is, how do we ensure that we're
         * not moving an interium page to a spot that also needs to move?
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
            long position = popFreeInterimPage();
            if (position == 0L)
            {
                position = fromWilderness();
            }
            return position;
        }
        
        /**
         * Remove the interim page from the set of free interim pages if the
         * page is in the set of free interim pages. Returns true if the page
         * was in the set of free interim pages.
         * <p>
         * This method can only be called while holding the expand lock in the
         * pager class.
         *
         * @param position The position of the iterim free page.
         */
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

        public void relocate(MoveLatch head)
        {
            synchronized (mapOfPagesByPosition)
            {
                Position position = removePageByPosition(head.getMove().getFrom());
                if (position != null)
                {
                    assert head.getMove().getTo() == position.getValue();
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

        public void add(Position page)
        {
            // FIXME Make calls to flush explicit.
            mapOfPages.put(page.getValue(), page);
            mapOfByteBuffers.put(page.getValue(), page.getByteBuffer());
            if (mapOfPages.size() > capacity)
            {
                flush();
            }
        }
        
        public void flush(Pointer pointer)
        {
            flush();
            FileChannel fileChannel = pager.getFileChannel();
            synchronized (pointer.getMutex())
            {
                ByteBuffer bytes = pointer.getByteBuffer();
                bytes.clear();
                try
                {
                    fileChannel.write(bytes, pointer.getPosition());
                }
                catch (IOException e)
                {
                    throw new Danger("io.write", e);
                }
            }
        }

        public void flush()
        {
            FileChannel fileChannel = pager.getFileChannel();
            for (Position position: mapOfPages.values())
            {
                synchronized (position)
                {
                    ByteBuffer bytes = position.getByteBuffer();
                    bytes.clear();
                    try
                    {
                        fileChannel.write(bytes, position.getValue());
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

    private final static class PositionSet
    {
        private final boolean[] reserved;

        private final long position;

        public PositionSet(long position, int count)
        {
            this.position = position;
            this.reserved = new boolean[count];
        }

        public synchronized Pointer allocate()
        {
            Pointer pointer = null;
            for (;;)
            {
                for (int i = 0; i < reserved.length && pointer == null; i++)
                {
                    if (!reserved[i])
                    {
                        reserved[i] = true;
                        pointer = new Pointer(ByteBuffer.allocateDirect(POSITION_SIZE), position + i * POSITION_SIZE, this);
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

        public synchronized void free(Pointer pointer)
        {
            int offset = (int) (pointer.getPosition() - position) / POSITION_SIZE;
            reserved[offset] = false;
            notify();
        }
        
        public int getCapacity()
        {
            return reserved.length;
        }
    }

    private abstract static class Operation
    {
        public void commit(Player player)
        {
        }

        public JournalPage getJournalPage(Player player, JournalPage journalPage)
        {
            return journalPage;
        }

        public boolean write(Pager pager, long destination, ByteBuffer data, DirtyPageMap pages)
        {
            return false;
        }

        public boolean unwrite(JournalPage journalPage, long destination)
        {
            return false;
        }

        public boolean terminate()
        {
            return false;
        }

        public abstract int length();

        public abstract void write(ByteBuffer bytes);

        public abstract void read(ByteBuffer bytes);
    }

    private final static class AddVacuum
    extends Operation
    {
        private long position;
        
        public AddVacuum()
        {
        }
        
        public AddVacuum(long position)
        {
            this.position = position;
        }
        
        @Override
        public void commit(Player player)
        {
            player.getVacuumPageSet().add(position);
        }
        
        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(VACUUM);
            bytes.putLong(position);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            this.position = bytes.getLong();
        }
    }

    private final static class Terminate
    extends Operation
    {
        @Override
        public int length()
        {
            return FLAG_SIZE;
        }

        @Override
        public boolean terminate()
        {
            return true;
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(TERMINATE);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
        }
    }

    private final static class Vacuum
    extends Operation
    {
        private long newJournalPosition;
        
        public Vacuum()
        {
        }
        
        public Vacuum(long position)
        {
            this.newJournalPosition = position;
        }

        @Override
        public void commit(Player player)
        {
            for (long position: player.getVacuumPageSet())
            {
                DataPage dataPage = player.getPager().getPage(position, new DataPage());
                dataPage.compact();
            }
            ByteBuffer bytes = player.getJournalHeader().getByteBuffer();
            bytes.clear();
            long oldJournalPosition = bytes.getLong();
            if (oldJournalPosition != newJournalPosition)
            {
                bytes.clear();
                bytes.putLong(newJournalPosition);
                player.getDirtyPages().flush(player.getJournalHeader());
            }
        }

        @Override
        public int length()
        {
            return FLAG_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(VACUUM);
            bytes.putLong(newJournalPosition);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            this.newJournalPosition = bytes.getLong();
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

        public void commit(Pager pager, Journal journal, DirtyPageMap dirtyPages)
        {
            AddressPage toAddressPage = pager.getPage(destination, new AddressPage());
            long toPosition = toAddressPage.dereference(destination);
            DataPage toDataPage = pager.getPage(toPosition, new DataPage());
            ByteBuffer fromBytes = journal.read(source, destination);
            toDataPage.write(toPosition, destination, fromBytes, dirtyPages);
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

    private final static class AddMove
    extends Operation
    {
        private Move move;

        public AddMove()
        {
        }

        public AddMove(Move move)
        {
            this.move = move;
        }
        
        @Override
        public void commit(Player player)
        {
            player.getMoveList().add(move);
        }

        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 2;
        }

        @Override
        public void read(ByteBuffer bytes)
        {
            move = new Move(bytes.getLong(), bytes.getLong());
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(ADD_MOVE);
            bytes.putLong(move.getFrom());
            bytes.putLong(move.getTo());
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

        public void commit(Pager pager, Journal journal, DirtyPageMap dirtyPages)
        {
            AddressPage addressPage = pager.getPage(address, new AddressPage());
            long referenced = addressPage.dereference(address);
            DataPage dataPage = pager.getPage(referenced, new DataPage());
            dataPage.free(address, dirtyPages);
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
        
        @Override
        public JournalPage getJournalPage(Player player, JournalPage journalPage)
        {
            journalPage = player.getPager().getPage(position, new JournalPage());
            journalPage.seek(position);
            return journalPage;
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
        private long interim;
        
        private long data;
        
        public Commit()
        {
        }

        public Commit(long interim, long data)
        {
            this.interim = interim;
            this.data = data;
        }
        
        @Override
        public void commit(Player player)
        {
            DataPage interimPage = player.getPager().getPage(interim, new DataPage());
            DataPage dataPage = player.getPager().getPage(data, new DataPage());
            interimPage.commit(dataPage, player.getDirtyPages());
        }

        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 2;
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(COMMIT);
            bytes.putLong(interim);
            bytes.putLong(data);
        }

        @Override
        public void read(ByteBuffer bytes)
        {
            this.interim = bytes.getLong();
            this.data = bytes.getLong();
        }
    }

    private static class Journal
    {
        private final Pager pager;
        
        private final PageRecorder pageRecorder;

        private final DirtyPageMap dirtyPages;

        private final MovablePosition journalStart;

        private JournalPage journalPage;

        /**
         * Create a new journal to record mutations. The dirty page map will
         * record the creation of wilderness pages.
         * <p>
         * The move node is necessary to create a movable position that will
         * track the position of journal head. This move node is the same move
         * node that is at the head of the mutator.
         * <p>
         * We will use the page recorder to record which pages we're using as
         * journal pages.
         * 
         * @param pager
         *            The pager of the mutator for the journal.
         * @param pageRecorder
         *            Records the allocation of new journal pages.
         * @param moveNode
         *            Needed to create a movable position that will reference
         *            the first journal page.
         * @param dirtyPages
         *            A dirty page map where page writes are cached before
         *            being written to disk.
         */
        public Journal(Pager pager, PageRecorder pageRecorder, MoveNode moveNode, DirtyPageMap dirtyPages)
        {
            this.journalPage = pager.newSystemPage(new JournalPage(), dirtyPages);
            this.journalStart = new MovablePosition(moveNode, journalPage.getJournalPosition());
            this.pager = pager;
            this.dirtyPages = dirtyPages;
            this.pageRecorder = pageRecorder;
            this.pageRecorder.getInterimPageSet().add(journalPage.getPosition().getValue());
        }
        
        public MovablePosition getJournalStart()
        {
            return journalStart;
        }
        
        public long getJournalPosition()
        {
            return journalPage.getJournalPosition();
        }

        public Pager getPager()
        {
            return pager;
        }

        public void shift(long position, long address, ByteBuffer bytes)
        {
            DataPage dataPage = pager.getPage(position, new DataPage());
            dataPage.write(position, address, bytes, dirtyPages);
        }

        public void write(long position, long address, ByteBuffer bytes)
        {
            DataPage dataPage = pager.getPage(position, new DataPage());
            dataPage.write(position, address, bytes, dirtyPages);
        }

        public ByteBuffer read(long position, long address)
        {
            DataPage dataPage = pager.getPage(position, new DataPage());
            return dataPage.read(position, address);
        }

        public void write(Operation operation)
        {
            if (!journalPage.write(operation, dirtyPages))
            {
                JournalPage nextJournalPage = pager.newSystemPage(new JournalPage(), dirtyPages);
                journalPage.write(new NextOperation(nextJournalPage.getJournalPosition()), dirtyPages);
                journalPage = nextJournalPage;
                pageRecorder.getInterimPageSet().add(journalPage.getPosition().getValue());
                write(operation);
            }
        }
    }

    private final static class Player
    {
        private final Pager pager;

        private final Pointer header;

        private final DirtyPageMap dirtyPages;
        
        private final Set<Long> setOfVacuumPages; 
        
        private final LinkedList<Move> listOfMoves;
        
        private long entryPosition;

        public Player(Pager pager, Pointer header, DirtyPageMap dirtyPages)
        {
            ByteBuffer bytes = header.getByteBuffer();
            
            bytes.clear();
            
            this.pager = pager;
            this.header = header;
            this.entryPosition = bytes.getLong();
            this.listOfMoves = new LinkedList<Move>();
            this.setOfVacuumPages = new HashSet<Long>();
            this.dirtyPages = dirtyPages;
        }
        
        public Pager getPager()
        {
            return pager;
        }
        
        public Pointer getJournalHeader()
        {
            return header;
        }
 
        public DirtyPageMap getDirtyPages()
        {
            return dirtyPages;
        }
        
        public LinkedList<Move> getMoveList()
        {
            return listOfMoves;
        }
        
        public Set<Long> getVacuumPageSet()
        {
            return setOfVacuumPages;
        }

        private Operation execute()
        {
            JournalPage journalPage = pager.getPage(entryPosition, new JournalPage());
            
            journalPage.seek(entryPosition);
            
            Operation operation = journalPage.next(); 
            while (!operation.terminate())
            {
                operation.commit(this);
                journalPage = operation.getJournalPage(this, journalPage);
                operation = journalPage.next();
            }

            entryPosition = journalPage.getJournalPosition();
            
            return operation;
        }

        public void vacuum()
        {
            Operation operation = execute();
            
            assert operation instanceof Terminate;
        }

        public void commit()
        {
            Operation operation = execute();
            
            pager.getJournalHeaderSet().free(header);
            
            assert operation instanceof Terminate;
        }
    }
    
    public final static class NullMoveRecorder
    implements MoveRecorder
    {
        public boolean contains(long position)
        {
            return false;
        }

        public void record(Move move)
        {
        }
    }
    
    public final static class MutateMoveRecorder
    implements MoveRecorder
    {
        private final Journal journal;

        private final PageRecorder pageRecorder;
        
        private final MoveNode firstMoveNode;

        private MoveNode moveNode;
        
        public MutateMoveRecorder(PageRecorder pageRecorder, Journal journal, MoveNode moveNode)
        {
            this.journal = journal;
            this.pageRecorder = pageRecorder;
            this.firstMoveNode = moveNode;
            this.moveNode = moveNode;
        }

        public boolean contains(long position)
        {
            return pageRecorder.contains(position);
        }

        public void record(Move move)
        {
            pageRecorder.record(move);
            moveNode = moveNode.extend(move);
            journal.write(new ShiftMove());
        }
        
        public MoveNode getFirstMoveNode()
        {
            return firstMoveNode;
        }
        
        public MoveNode getMoveNode()
        {
            return moveNode;
        }
        
        public PageRecorder getPageRecorder()
        {
            return pageRecorder;
        }
    }

    public final static class CommitMoveRecorder
    implements MoveRecorder
    {
        private final Journal journal;
        
        private final PageRecorder pageRecorder;
        
        private final Set<Long> setOfDataPages;

        private final SortedMap<Long, MovablePosition> mapOfVacuums;
        
        private final SortedMap<Long, MovablePosition> mapOfPages;
        
        private final MoveNode firstMoveNode;

        private MoveNode moveNode;
        
        public CommitMoveRecorder(PageRecorder pageRecorder, Journal journal, MoveNode moveNode)
        {
            this.pageRecorder = pageRecorder;
            this.journal = journal;
            this.setOfDataPages = new HashSet<Long>();
            this.mapOfVacuums = new TreeMap<Long, MovablePosition>();
            this.mapOfPages = new TreeMap<Long, MovablePosition>();
            this.firstMoveNode = moveNode;
            this.moveNode = moveNode;
        }
        
        public boolean contains(long position)
        {
            return pageRecorder.contains(position)
                || setOfDataPages.contains(position)
                || setOfDataPages.contains(-position)
                || mapOfVacuums.containsKey(position)
                || mapOfPages.containsKey(position);
        }
        
        public void record(Move move)
        {
            boolean moved = pageRecorder.contains(move.getFrom());
            pageRecorder.record(move);
            if (mapOfVacuums.containsKey(move.getFrom()))
            {
                mapOfVacuums.put(move.getTo(), mapOfVacuums.remove(move.getFrom()));
                moved = true;
            }
            if (mapOfPages.containsKey(move.getFrom()))
            {
                mapOfPages.put(move.getTo(), mapOfPages.remove(move.getFrom()));
                moved = true;
            }
            if (setOfDataPages.remove(move.getFrom()))
            {
                setOfDataPages.add(move.getTo());
                moved = true;
            }
            if (setOfDataPages.remove(-move.getFrom()))
            {
                setOfDataPages.add(move.getFrom());
            }
            if (moved)
            {
                moveNode = moveNode.extend(move);
                journal.write(new ShiftMove());
            }
        }
        
        public MoveNode getFirstMoveNode()
        {
            return firstMoveNode;
        }
        
        public Set<Long> getDataPageSet()
        {
            return setOfDataPages;
        }

        public SortedMap<Long, MovablePosition> getVacuumMap()
        {
            return mapOfVacuums;
        }
        
        public SortedMap<Long, MovablePosition> getPageMap()
        {
            return mapOfPages;
        }
        
        public PageRecorder getPageRecorder()
        {
            return pageRecorder;
        }
    }

    public final static class Mutator
    {
        private final Pager pager;
        
        private final Journal journal;

        private final BySizeTable allocPagesBySize;
        
        private final BySizeTable writePagesBySize;

        private final Map<Long, MovablePosition> mapOfAddresses;

        private long lastPointerPage;

        private final DirtyPageMap dirtyPages;
        
        private MutateMoveRecorder moveRecorder;
        
        private final PageRecorder pageRecorder;
        
        private final MoveList listOfMoves;

        public Mutator(Pager pager, MoveList listOfMoves, PageRecorder pageRecorder, Journal journal, MoveNode moveNode, DirtyPageMap dirtyPages)
        {
            this.pager = pager;
            this.journal = journal;
            this.allocPagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.writePagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.dirtyPages = dirtyPages;
            this.mapOfAddresses = new HashMap<Long, MovablePosition>();
            this.moveRecorder = new MutateMoveRecorder(pageRecorder, journal, moveNode);
            this.listOfMoves = new MoveList(moveRecorder, listOfMoves);
            this.pageRecorder = pageRecorder;
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
                    
            final int fullSize = blockSize + BLOCK_HEADER_SIZE;
           
            return listOfMoves.mutate(new Returnable<Long>()
            {
                public Long run()
                {
                    
                    // This is unimplemented: Creating a linked list of blocks
                    // when the block size exceeds the size of a page.
                    
                    int pageSize = pager.getPageSize();
                    if (fullSize + DATA_PAGE_HEADER_SIZE > pageSize)
                    {
                        // Recurse.
                    }
                    
                    // If we already have a wilderness data page that will fit
                    // the block, use that page. Otherwise, allocate a new
                    // wilderness data page for allocation.
                    
                    DataPage allocPage = null;
                    long alloc = allocPagesBySize.bestFit(fullSize);
                    if (alloc == 0L)
                    {
                        allocPage = pager.newSystemPage(new DataPage(), dirtyPages);
                        moveRecorder.getPageRecorder().getAllocationPageSet().add(allocPage.getPosition().getValue());
                    }
                    else
                    {
                        allocPage = pager.getPage(alloc, new DataPage());
                    }
                    
                    // FIXME This is where we split things up so we can grow
                    // the data page sections. 
                    
                    // Reserve an address.
                    
                    AddressPage addressPage = null;
                    long address = 0L;
                    do
                    {
                        addressPage = pager.getPage(lastPointerPage, new AddressPage());
                        addressPage.getLock().lock();
                        try
                        {
                            address = addressPage.reserve(dirtyPages);
                        }
                        finally
                        {
                            addressPage.getLock().unlock();
                        }
                        if (address == 0L)
                        {
                            // Allocate a different page.
                            address = pager.reserve(addressPage, dirtyPages);
                        }
                    }
                    while (address == 0L);
                    
                    // FIXME After we have the address, we need to go 
                    // back into the move list.
                    
                    // Allocate a block from the wilderness data page.
                    
                    long position = allocPage.allocate(address, fullSize, dirtyPages);
                    
                    allocPagesBySize.add(allocPage);
                    
                    mapOfAddresses.put(address, new MovablePosition(moveRecorder.getMoveNode(), position));
                    
                    return address;
                }
            });
        }

        public void commit()
        {
            // TODO I believe that compacting should be a separate lock that
            // should lock the entire file exclusively and block any other moves
            // or commits. I'm working under the assumption that the positions
            // do not move backwards. Compact lock may only need to envelop the
            // exiting move lock. 

            pager.getCompactLock().readLock().lock();
            try
            {
                tryCommit();
            }
            finally
            {
                pager.getCompactLock().readLock().unlock();
            }
        }
        
        private void tryCommit()
        {
            final CommitMoveRecorder commit = new CommitMoveRecorder(pageRecorder, journal, moveRecorder.getMoveNode());

            // FIXME Not all the pages in the by size map are allocations. Some
            // of them are writes.

            // First we mate the interim data pages with 

            listOfMoves.mutate(new Runnable()
            {
                public void run()
                {
                    // Consolidate pages by using existing, partially filled
                    // pages to store our new block allocations.

                    pager.pagesBySize.join(allocPagesBySize, commit.getDataPageSet(), commit.getVacuumMap(), moveRecorder.getMoveNode());
                    pageRecorder.getAllocationPageSet().removeAll(commit.getVacuumMap().keySet());
                    
                    // Use free data pages to store the interim pages whose
                    // blocks would not fit on an existing page.
                    
                    pager.newUserDataPages(pageRecorder.getAllocationPageSet(), commit.getDataPageSet(), commit.getPageMap(), moveRecorder.getMoveNode());
                }
            });


            // If more pages are needed, then we need to extend the user area of
            // the file.

            if (pageRecorder.getAllocationPageSet().size() != 0)
            {
                pager.getExpandLock().lock();
                try
                {
                    // This invocation is to flush the move list for the current
                    // mutator. You may think that this is pointless, but it's
                    // not. It will ensure that the relocatable references are
                    // all up to date before we try to move.

                    new MoveList(commit, listOfMoves).mutate(new Runnable()
                    {
                        public void run()
                        {
                        }
                    });

                    // Now we can try to move the pages.

                    tryMove(commit);
                }
                finally
                {
                    pager.getExpandLock().unlock();
                }
            }
            
            new MoveList(commit, listOfMoves).mutate(new Runnable()
            {
                public void run()
                {
                    journal.write(new Terminate());

                    // Grab the position. This is where we start.
                    long beforeVacuum = journal.getJournalPosition();
                    
                    // Create an operation for all the vacuums.
                    for (Map.Entry<Long, MovablePosition> entry: commit.getVacuumMap().entrySet())
                    {
                        // FIXME This has not run yet.
                        DataPage dataPage = pager.getPage(entry.getValue().getValue(pager), new DataPage());
                        dataPage.mirror(pager, journal, dirtyPages);
                    }

                    long afterVacuum = journal.getJournalPosition(); 
                    
                    // Write out all your allocations from above. Each of them
                    // becomes an action. Read the interim page and copy the
                    // data over to the data page.
                    
                    // Here we insert the vacuum break. During a recovery, the
                    // data pages will be recreated without a reference to their
                    // vacuumed page.

                    // Although, I suppose the vacuum page reference could
                    // simply be a reference to the data page.

                    // Two ways to deal with writing to a vacuumed page. One it
                    // to overwrite the vacuum journal. The other is to wait
                    // until the vacuumed journal is written.
                    journal.write(new Vacuum(afterVacuum));
                    
                    journal.write(new Terminate());
                    
                    for (Map.Entry<Long, MovablePosition> entry: commit.getVacuumMap().entrySet())
                    {
                        journal.write(new Commit(entry.getKey(), entry.getValue().getValue(pager)));
                    }
                    
                    for (Map.Entry<Long, MovablePosition> entry: commit.getPageMap().entrySet())
                    {
                        journal.write(new Commit(entry.getKey(), entry.getValue().getValue(pager)));
                    }

                    // Create the list of moves.
                    MoveNode iterator = moveRecorder.getFirstMoveNode();
                    while (iterator.getNext() != null)
                    {
                        iterator = iterator.getNext();
                        journal.write(new AddMove(iterator.getMove()));
                    }

                    // Need to use the entire list of moves since the start
                    // of the journal to determine the actual journal start.
                    
                    long journalStart = journal.getJournalStart().getValue(pager);
                    journal.write(new NextOperation(journalStart));

                    // TODO Abstract journal replay out, so it can be used
                    // here and during recovery.
                    
                    // Create a next pointer to point at the start of operations.
                    Pointer header = pager.getJournalHeaderSet().allocate();
                    header.getByteBuffer().putLong(beforeVacuum);
                    dirtyPages.flush(header);
                    
                    Player player = new Player(pager, header, dirtyPages);
                    
                    // Obtain a journal header and record the head.
                    
                    // First do the vacuums.
                    player.vacuum();
                    
                    // Then do everything else.
                    player.commit();
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
        private long gatherPages(CommitMoveRecorder commit, Set<Long> setOfInUse, Set<Long> setOfGathered)
        {
            long position = pager.getFirstInterimPage();

            while (pageRecorder.getAllocationPageSet().size() != 0)
            {
                if (!pager.removeInterimPageIfFree(position))
                {
                    setOfInUse.add(position);
                }

                long from = pageRecorder.getAllocationPageSet().iterator().next();
                pageRecorder.getAllocationPageSet().remove(from);
                
                commit.getDataPageSet().add(-position);
                commit.getPageMap().put(from, new SkippingMovablePosition(moveRecorder.getMoveNode(), position));
                
                setOfGathered.add(position);

                position += pager.getPageSize();
            }

            return position;
        }
        
        private void tryMove(MoveLatch head)
        {
            while (head != null)
            {
                RelocatablePage page = pager.getPage(head.getMove().getFrom(), new RelocatablePage());
                page.relocate(head.getMove().getTo());
                // TODO Curious about this. Will this get out of sync? Do I have
                // things locked down enough?
                
                // Anyone referencing the page is going to be waiting for the
                // move to complete. 
                
                // Need to look at each method in mutator and assure myself that
                // no one else is reading the pager's page map. The
                // synchronization of the page map is needed between mutators
                // for ordinary tasks. Here, though, everyone else should be all
                // locked down.
                pager.relocate(head);
                
                head.getLock().unlock();
                
                head = head.getNext();
            }  
        }

        private Set<Long> tryMove(CommitMoveRecorder commit)
        {
            SortedSet<Long> setOfInUse = new TreeSet<Long>();
            SortedSet<Long> setOfGathered = new TreeSet<Long>();
            
            long firstIterimPage = gatherPages(commit, setOfInUse, setOfGathered);

            // For the set of pages in use, add the page to the move list.

            MoveLatch head = null;
            MoveLatch move = null;
            Iterator<Long> inUse = setOfInUse.iterator();
            if (inUse.hasNext())
            {
                long from = inUse.next();
                head = move = new MoveLatch(new Move(from, pager.newBlankInterimPage()), null);
            }
            while (inUse.hasNext())
            {
                long from = inUse.next();
                move = new MoveLatch(new Move(from, pager.newBlankInterimPage()), move);
            }

            if (head != null)
            {
                pager.getMoveList().add(move);
            
                // At this point, no one else is moving because we have a
                // monitor that only allows one mutator to move at once. Other
                // mutators may be referencing pages that are moved. Appending
                // to the move list blocked referencing mutators with a latch on
                // each move. We are clear to move the pages that are in use.

                tryMove(head);
            }
            
            pager.setFirstInterimPage(firstIterimPage);
            
            for (long gathered: setOfGathered)
            {
                pager.setPage(gathered, new DataPage(), dirtyPages);
            }
            
            return setOfGathered;
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
                    
                    DataPage writePage = null;
                    MovablePosition position = mapOfAddresses.get(address);
                    if (position == null)
                    {
                        AddressPage addressPage = pager.getPage(address, new AddressPage());
                        DataPage dataPage = pager.getPage(addressPage.dereference(address), new DataPage());
                        
                        int length = dataPage.getSize(position, address);
                        
                        long write = writePagesBySize.bestFit(length);
                        if (write == 0L)
                        {
                            writePage = pager.newSystemPage(new DataPage(), dirtyPages);
                            moveRecorder.getPageRecorder().getAllocationPageSet().add(write);
                        }
                        else
                        {
                            writePage = pager.getPage(write, new DataPage());
                        }
                        
                        writePage.allocate(address, length, dirtyPages);
                        
                        position = new MovablePosition(moveRecorder.getMoveNode(), write);
                        mapOfAddresses.put(address, position);
                    }
                    else
                    {
                        writePage  = pager.getPage(position.getValue(pager), new DataPage());
                    }

                    writePage.write(position.getValue(pager), address, bytes, dirtyPages);
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
