/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
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
    public final static int ERROR_FILE_NOT_FOUND = 400;
    
    public final static int ERROR_IO_WRITE = 401;

    public final static int ERROR_IO_READ = 402;

    public final static int ERROR_IO_SIZE = 403;

    public final static int ERROR_IO_TRUNCATE = 404;

    public final static int ERROR_IO_FORCE = 405;

    public final static int ERROR_IO_CLOSE = 406;

    public final static int ERROR_IO_STATIC_PAGES = 407;
    
    public final static int ERROR_SIGNATURE = 501;

    public final static int ERROR_SHUTDOWN = 502;

    public final static int ERROR_FILE_SIZE = 503;

    private final static long SIGNATURE = 0xAAAAAAAAAAAAAAAAL;
    
    private final static int SOFT_SHUTDOWN = 0xAAAAAAAA;

    private final static int HARD_SHUTDOWN = 0x55555555;
    
    private final static int FLAG_SIZE = 2;

    private final static int COUNT_SIZE = 4;

    private final static int POSITION_SIZE = 8;

    private final static int CHECKSUM_SIZE = 8;

    private final static int ADDRESS_SIZE = 8;

    private final static int FILE_HEADER_SIZE = COUNT_SIZE * 5 + ADDRESS_SIZE * 4;

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

    private final static int ADDRESS_PAGE_HEADER_SIZE = CHECKSUM_SIZE;

    private final static int JOURNAL_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;
    
    private final static int COUNT_MASK = 0xA0000000;

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
        pager.close();
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
        
        private final int code;

        public Danger(int code)
        {
            super(Integer.toString(code));
            this.code = code;
        }

        public Danger(int code, Throwable cause)
        {
            super(Integer.toString(code), cause);
            this.code = code;
        }
        
        public int getCode()
        {
            return code;
        }
    }
    
    static abstract class Regional
    {
        private long position;
        
        final SortedMap<Long, Long> setOfRegions;
        
        public Regional(long position)
        {
            this.position = position;
            this.setOfRegions = new TreeMap<Long, Long>();
        }
        
        public abstract ByteBuffer getByteBuffer();

        /**
         * Get the page size boundary aligned position of the page in file.
         * 
         * @return The position of the page.
         */
        public synchronized long getPosition()
        {
            return position;
        }

        /**
         * Set the page size boundary aligned position of the page in file.
         * 
         * @param position The position of the page.
         */
        public synchronized void setPosition(long position)
        {
            this.position = position;
        }

        public void invalidate(int offset, int length)
        {
            long start = getPosition() + offset;
            long end = start + length;
            invalidate(start, end);
        }
        
        private void invalidate(long start, long end)
        {
            assert start >= getPosition();
            assert end <= getPosition() + getByteBuffer().capacity();
            
            INVALIDATE: for(;;)
            {
                Iterator<Map.Entry<Long, Long>> entries = setOfRegions.entrySet().iterator();
                while (entries.hasNext())
                {
                    Map.Entry<Long, Long> entry = entries.next();
                    if (start < entry.getKey() && end >= entry.getKey())
                    {
                        entries.remove();
                        end = end > entry.getValue() ? end : entry.getValue();
                        continue INVALIDATE;
                    }
                    else if (entry.getKey() <= start && start <= entry.getValue())
                    {
                        entries.remove();
                        start = entry.getKey();
                        end = end > entry.getValue() ? end : entry.getValue();
                        continue INVALIDATE;
                    }
                    else if (entry.getValue() < start)
                    {
                        break;
                    }
                }
                break;
            }
            setOfRegions.put(start, end);
        }
        
        public void write(Disk disk, FileChannel fileChannel) throws IOException
        {
            ByteBuffer bytes = getByteBuffer();
            for(Map.Entry<Long, Long> entry: setOfRegions.entrySet())
            {
                int offset = (int) (entry.getKey() - getPosition());
                int length = (int) (entry.getValue() - entry.getKey());

                bytes.clear();
                
                bytes.position(offset);
                bytes.limit(offset + length);
                
                disk.write(fileChannel, bytes, entry.getKey());
            }
            setOfRegions.clear();
        }
    }
     
    private final static class Header extends Regional
    {
        private final ByteBuffer bytes;
        
        public Header(ByteBuffer bytes)
        {
            super(0L);
            this.bytes = bytes;
        }
        
        public ByteBuffer getByteBuffer()
        {
            return bytes;
        }
        
        public long getStaticPagesStart()
        {
            return FILE_HEADER_SIZE + getInternalJournalCount() * POSITION_SIZE;
        }

        public long getSignature()
        {
            return bytes.getLong(0);
        }
        
        public void setSignature(long signature)
        {
            bytes.putLong(0, signature);
            invalidate(0, CHECKSUM_SIZE);
        }
        
        public int getShutdown()
        {
            return bytes.getInt(CHECKSUM_SIZE);
        }
        
        public void setShutdown(int shutdown)
        {
            bytes.putInt(CHECKSUM_SIZE, shutdown);
            invalidate(CHECKSUM_SIZE, COUNT_SIZE);
        }
        
        public int getPageSize()
        {
            return bytes.getInt(CHECKSUM_SIZE + COUNT_SIZE);
        }
        
        public void setPageSize(int pageSize)
        {
            bytes.putInt(CHECKSUM_SIZE + COUNT_SIZE, pageSize);
            invalidate(CHECKSUM_SIZE + COUNT_SIZE, COUNT_SIZE);
        }
        
        public int getAlignment()
        {
            return bytes.getInt(CHECKSUM_SIZE + COUNT_SIZE * 2);
        }
        
        public void setAlignment(int alignment)
        {
            bytes.putInt(CHECKSUM_SIZE + COUNT_SIZE * 2, alignment);
            invalidate(CHECKSUM_SIZE + COUNT_SIZE * 2, COUNT_SIZE);
        }
        
        public int getInternalJournalCount()
        {
            return bytes.getInt(CHECKSUM_SIZE + COUNT_SIZE * 3);
        }
        
        public void setInternalJournalCount(int internalJournalCount)
        {
            bytes.putInt(CHECKSUM_SIZE + COUNT_SIZE * 3, internalJournalCount);
            invalidate(CHECKSUM_SIZE + COUNT_SIZE * 3, COUNT_SIZE);
        }
        
        public int getStaticPageSize()
        {
            return bytes.getInt(CHECKSUM_SIZE + COUNT_SIZE * 4);
        }
        
        public void setStaticPageSize(int staticPageSize)
        {
            bytes.putInt(CHECKSUM_SIZE + COUNT_SIZE * 4, staticPageSize);
            invalidate(CHECKSUM_SIZE + COUNT_SIZE * 4, COUNT_SIZE);
        }
        
        public long getFirstAddressPageStart()
        {
            return bytes.getLong(CHECKSUM_SIZE + COUNT_SIZE * 5);
        }
        
        public void setFirstAddressPageStart(long firstAddressPageStart)
        {
            bytes.putLong(CHECKSUM_SIZE + COUNT_SIZE * 5, firstAddressPageStart);
            invalidate(CHECKSUM_SIZE + COUNT_SIZE * 5, ADDRESS_SIZE);
        }

        public long getDataBoundary()
        {
            return bytes.getLong(CHECKSUM_SIZE * 2 + COUNT_SIZE * 5);
        }
        
        public void setDataBoundary(long dataBoundary)
        {
            bytes.putLong(CHECKSUM_SIZE * 2 + COUNT_SIZE * 5, dataBoundary);
            invalidate(CHECKSUM_SIZE * 2 + COUNT_SIZE * 5, ADDRESS_SIZE);
        }
        
        public long getOpenBoundary()
        {
            return bytes.getLong(CHECKSUM_SIZE * 3 + COUNT_SIZE * 5);
        }
        
        public void setOpenBoundary(long openBoundary)
        {
            bytes.putLong(CHECKSUM_SIZE * 3 + COUNT_SIZE * 5, openBoundary);
            invalidate(CHECKSUM_SIZE * 3 + COUNT_SIZE * 5, ADDRESS_SIZE);
        }
    }

    public final static class Creator
    {
        private final Map<URI, Integer> mapOfStaticPageSizes;

        private int pageSize;

        private int alignment;

        private int internalJournalCount;
        
        private Disk disk;

        public Creator()
        {
            this.mapOfStaticPageSizes = new TreeMap<URI, Integer>();
            this.pageSize = 8 * 1024;
            this.alignment = 64;
            this.internalJournalCount = 64;
            this.disk = new Disk();
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
        
        public void setDisk(Disk disk)
        {
            this.disk = disk;
        }

        public void addStaticPage(URI uri, int blockSize)
        {
            mapOfStaticPageSizes.put(uri, blockSize);
        }
        
        private int getStaticPagesMapSize()
        {
            int size = COUNT_SIZE;
            for (Map.Entry<URI, Integer> entry: mapOfStaticPageSizes.entrySet())
            {
                size += COUNT_SIZE + ADDRESS_SIZE;
                size += entry.getValue().toString().length();
            }
            return size;
        }

        private long getEndOfHeader()
        {
            return FILE_HEADER_SIZE + getStaticPagesMapSize() + internalJournalCount * POSITION_SIZE; 
        }
        
        private long getFirstAddressPageStart()
        {
            long endOfHeader = getEndOfHeader();
            long remaining = pageSize - endOfHeader % pageSize;
            if (remaining < ADDRESS_PAGE_HEADER_SIZE + ADDRESS_SIZE)
            {
                return  (endOfHeader + pageSize - 1) / pageSize * pageSize;
            }
            return endOfHeader;
        }
        
        private long getFirstAddressPage()
        {
            return getFirstAddressPageStart() / pageSize * pageSize;
        }

        private long getDataBoundary()
        {
            return (getFirstAddressPageStart() + pageSize - 1) / pageSize * pageSize;
        }

        /**
         * Create a new pack that writes to the specified file.
         */
        public Pack create(File file)
        {
            // Open the file.
            
            FileChannel fileChannel;
            try
            {
                fileChannel = disk.open(file);
            }
            catch (FileNotFoundException e)
            {
                throw new Danger(ERROR_FILE_NOT_FOUND, e);
            }
            
            ByteBuffer fullSize = ByteBuffer.allocateDirect((int) (getFirstAddressPage() + pageSize));
            try
            {
                disk.write(fileChannel, fullSize, 0L);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_WRITE, e);
            }
            
            // Initialize the header.
            
            Header header = new Header(ByteBuffer.allocateDirect(FILE_HEADER_SIZE));
            
            header.setSignature(SIGNATURE);
            header.setShutdown(HARD_SHUTDOWN);
            header.setPageSize(pageSize);
            header.setAlignment(alignment);
            header.setInternalJournalCount(internalJournalCount);
            header.setStaticPageSize(getStaticPagesMapSize());
            header.setFirstAddressPageStart(getFirstAddressPageStart());
            header.setDataBoundary(getDataBoundary());
            header.setOpenBoundary(0L);

            try
            {
                header.write(disk, fileChannel);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_WRITE, e);
            }

            // Create a buffer of journal file positions. Initialize each page
            // position to 0. Write the journal headers to file.

            ByteBuffer journals = ByteBuffer.allocateDirect(internalJournalCount * POSITION_SIZE);

            for (int i = 0; i < internalJournalCount; i++)
            {
                journals.putLong(0L);
            }

            journals.flip();

            try
            {
                fileChannel.write(journals, FILE_HEADER_SIZE);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_WRITE, e);
            }

            // To create the map of static pages, we're going to allocate a
            // block from the pager. We create a local pack for this purpose.
            // This local pack will have a bogus, empty map of static pages.
            // We create a subsequent pack to return to the user.

            Map<URI, Long> mapOfStaticPages = new HashMap<URI, Long>();

            SortedSet<Long> setOfAddressPages = new TreeSet<Long>();
            setOfAddressPages.add(getFirstAddressPage());
            Pager pager = new Pager(file, fileChannel, disk, header, mapOfStaticPages, setOfAddressPages, header.getDataBoundary());

            Pack pack = new Pack(pager);

            ByteBuffer statics = ByteBuffer.allocateDirect(getStaticPagesMapSize());
            
            statics.putInt(mapOfStaticPages.size());
            
            if (mapOfStaticPageSizes.size() != 0)
            {
                Mutator mutator = pack.mutate();
                for (Map.Entry<URI, Integer> entry: mapOfStaticPageSizes.entrySet())
                {
                    String uri = entry.getKey().toString();
                    int size = entry.getValue();
                    long address = mutator.allocate(size);
                    statics.putInt(uri.length());
                    for (int i = 0; i < uri.length(); i++)
                    {
                        statics.putChar(uri.charAt(i));
                    }
                    statics.putLong(address);
                }
                mutator.commit();
            }
            
            statics.flip();
            
            try
            {
                disk.write(fileChannel, statics, header.getStaticPagesStart());
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_WRITE, e);
            }

            pack.close();
            
            Opener opener = new Opener();
            
            opener.setDisk(disk);
            
            return opener.open(file);
        }
    }
    
    /**
     * Opens pack files and performs recovery.
     */
    public final static class Opener
    {
        private Disk disk;
        
        public Opener()
        {
            this.disk = new Disk();
        }
        
        public void setDisk(Disk disk)
        {
            this.disk = disk;
        }
        
        private boolean badAddress(Header header, long address)
        {
            return address < header.getFirstAddressPageStart() + ADDRESS_PAGE_HEADER_SIZE
                || address > header.getDataBoundary();
        }

        private Map<URI, Long> readStaticPages(Header header, FileChannel fileChannel) 
        {
            Map<URI, Long> mapOfStaticPages = new TreeMap<URI, Long>();
            ByteBuffer bytes = ByteBuffer.allocateDirect(header.getStaticPageSize());
            try
            {
                disk.read(fileChannel, bytes, header.getStaticPagesStart());
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_READ, e);
            }
            bytes.flip();
            int count = bytes.getInt();
            for (int i = 0; i < count; i++)
            {
                StringBuilder uri = new StringBuilder();
                int length = bytes.getInt();
                for (int j = 0; j < length; j++)
                {
                    uri.append(bytes.getChar());
                }
                long address = bytes.getLong();
                if (badAddress(header, address))
                {
                    throw new Danger(ERROR_IO_STATIC_PAGES);
                }
                try
                {
                    mapOfStaticPages.put(new URI(uri.toString()), address);
                }
                catch (URISyntaxException e)
                {
                    throw new Danger(ERROR_IO_STATIC_PAGES, e);
                }
            }
            return mapOfStaticPages;
        }    


        private Header readHeader(FileChannel fileChannel)
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(FILE_HEADER_SIZE);
            try
            {
                disk.read(fileChannel, bytes, 0L);
            }
            catch (IOException e)
            {
               throw new Danger(ERROR_IO_READ, e);
            }
            return new Header(bytes);
        }

        public Pack open(File file)
        {
            // Open the file channel.

            FileChannel fileChannel;
            try
            {
                fileChannel = disk.open(file);
            }
            catch (FileNotFoundException e)
            {
                throw new Danger(ERROR_FILE_NOT_FOUND, e);
            }

            // Read the header and obtain the basic file properties.

            Header header = readHeader(fileChannel);
       
            if (header.getSignature() != SIGNATURE)
            {
                throw new Danger(ERROR_SIGNATURE);
            }
            
            int shutdown = header.getShutdown();
            if (!(shutdown == HARD_SHUTDOWN || shutdown == SOFT_SHUTDOWN))
            {
                throw new Danger(ERROR_SHUTDOWN);
            }
            
            if (shutdown == HARD_SHUTDOWN)
            {
                throw new UnsupportedOperationException();
            }

            Map<URI, Long> mapOfStaticPages = readStaticPages(header, fileChannel);

            int reopenSize = 0;
            try
            {
                reopenSize = (int) (disk.size(fileChannel) - header.getOpenBoundary());
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_SIZE, e);
            }
            
            ByteBuffer reopen = ByteBuffer.allocateDirect(reopenSize);
            try
            {
                disk.read(fileChannel, reopen, header.getOpenBoundary());
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_READ, e);
            }
            reopen.flip();
            
            SortedSet<Long> setOfAddressPages = new TreeSet<Long>();
            
            int addressPageCount = reopen.getInt();
            for (int i = 0; i < addressPageCount; i++)
            {
                setOfAddressPages.add(reopen.getLong());
            }
            
            Pager pager = new Pager(file, fileChannel, disk, header, mapOfStaticPages, setOfAddressPages, header.getOpenBoundary());
            
            int blockPageCount = reopen.getInt();
            for (int i = 0; i < blockPageCount; i++)
            {
                long position = reopen.getLong();
                DataPage blockPage = pager.getPage(position, new DataPage(false));
                pager.returnUserPage(blockPage);
            }
            
            try
            {
                disk.truncate(fileChannel, header.getOpenBoundary());
            }
            catch (IOException e)
            {
                new Danger(ERROR_IO_TRUNCATE, e);
            }
            
            header.setShutdown(HARD_SHUTDOWN);
            header.setOpenBoundary(0L);

            try
            {
                header.write(disk, fileChannel);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_WRITE, e);
            }
            
            try
            {
                disk.force(fileChannel);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_FORCE, e);
            }


            return new Pack(pager);
       }
        
        // FIXME Not yet called anywhere.
        public void recovery(Pager pager)
        {
            Recovery recovery = new Recovery();
            long position = 0L;
            while (recovery.getRegion() != INTERIM_REGION)
            {
                pager.recover(recovery, position, new NonInterimMedic());
                position += pager.getPageSize();
            }
        }
    }

    private final static short ADDRESS_REGION = 1;
    
//    private final static short DATA_REGION = 2;
    
    private final static short INTERIM_REGION = 3;
    
    private final static class Recovery
    {
        private final Map<Long, Long> mapOfBadAddresses;
        
        private final Set<Long> setOfCorruptDataPages;
        
        private final Set<Long> setOfBadDataChecksums;
        
        private final Set<Long> setOfBadAddressChecksums;

        private final Checksum checksum;
        
        private short region;
        
        private int blockCount;
        
        public Recovery()
        {
            this.region = ADDRESS_REGION;
            this.checksum = new Adler32();
            this.mapOfBadAddresses = new HashMap<Long, Long>();
            this.setOfBadAddressChecksums = new HashSet<Long>();
            this.setOfBadDataChecksums = new HashSet<Long>();
            this.setOfCorruptDataPages = new HashSet<Long>();
        }
        
        public short getRegion()
        {
            return region;
        }
        
        public void setRegion(short region)
        {
            this.region = region;
        }
        
        public int getBlockCount()
        {
            return blockCount;
        }
        
        public void incBlockCount()
        {
            blockCount++;
        }
        
        public Checksum getChecksum()
        {
            return checksum;
        }

        public void badAddressChecksum(long position)
        {
            setOfBadAddressChecksums.add(position);
        }
        
        public void corruptDataPage(long position)
        {
            setOfCorruptDataPages.add(position);
        }

        public void badDataChecksum(long position)
        {
            setOfBadDataChecksums.add(position);
        }
        
        public void badAddress(long address, long position)
        {
            mapOfBadAddresses.put(address, position);
        }
        
        public long getFileSize()
        {
            return 0L;
        }
    }

    private interface Medic
    {
        public Page recover(RawPage rawPage, Recovery recovery);
    }

    private final static class NonInterimMedic implements Medic
    {
        public Page recover(RawPage rawPage, Recovery recovery)
        {
            Medic medic = null;
            long position = rawPage.getPosition();
            long first = rawPage.getPager().getFirstAddressPageStart();
            if (position < first)
            {
                int pageSize = rawPage.getPager().getPageSize();
                if (position == first / pageSize * pageSize)
                {
                    medic = new AddressPage();
                }
                else
                {
                    throw new IllegalStateException();
                }
            }
            else
            {
                ByteBuffer peek = rawPage.getByteBuffer();
                
                peek.clear();
                
                try
                {
                    Pager pager = rawPage.getPager();
                    pager.getDisk().read(pager.getFileChannel(), peek, position + CHECKSUM_SIZE);
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_READ, e);
                }

                if (peek.getInt(CHECKSUM_SIZE) < 0)
                {
                    medic = new DataPage(false);
                }
                else if (recovery.getBlockCount() == 0)
                {
                    medic = new AddressPage();
                }
                else
                {
                    recovery.setRegion(INTERIM_REGION);
                    return null;
                }
            }
            return medic.recover(rawPage, recovery);
        }
    }
    
    /**
     * Wrapper around calls to <code>FileChannel</code> methods that allows
     * for simulating IO failures in testing. The <code>Disk</code> is
     * stateless. Methods take a <code>FileChannel</code> as a parameter. The
     * default implementation forwards the calls directly to the
     * <code>FileChannel</code>.
     * <p>
     * This class is generally useful to testing. Users are encouraged to
     * subclass <code>Disk</code> to throw <code>IOException</code>
     * exceptions for their own unit tests. For example, subclass of disk can
     * inspect writes for a particular file position and fail after a certain
     * amount of writes.
     * <p>
     * The <code>Disk</code> class is set using the
     * {@link Pack.Creator#setDisk Pack.Creator.setDisk} or
     * {@link Pack.Opener#setDisk} methods. Because it is stateless it can be
     * used for multiple <code>Pack</code> instances. However the only
     * intended use case for a subclass of <code>Disk</code> to generate I/O
     * failures. These subclasses are not expected to be stateless.
     */
    public static class Disk
    {
        /**
         * Create an instance of <code>Disk</code>.
         */
        public Disk()
        {
        }

        public void objectIO() throws IOException
        {
        }

        /**
         * Open a file and create a <code>FileChannel</code>.
         * 
         * @param file
         *            The file to open.
         * @return A <code>FileChannel</code> open on the specified file.
         * @throws FileNotFoundException
         *             If the file exists but is a directory rather than a
         *             regular file, or cannot be opened or created for any
         *             other reason.
         */
        public FileChannel open(File file) throws FileNotFoundException
        {
            return new RandomAccessFile(file, "rw").getChannel();
        }

        /**
         * Writes a sequence of bytes to the file channel from the given buffer,
         * starting at the given file position.
         * 
         * @param fileChannel
         *            The file channel to which to write.
         * @param src
         *            The buffer from which bytes are transferred.
         * @param position
         *            The file position at which the transfer is to begin, must
         *            be non-negative.
         * @return The number of bytes written, possibly zero.
         * @throws IOException
         *             If an I/O error occurs.
         */
        public int write(FileChannel fileChannel, ByteBuffer src, long position) throws IOException
        {
            return fileChannel.write(src, position);
        }

        /**
         * Reads a sequence of bytes from the file channel into the given
         * buffer, starting at the given file position.
         * 
         * @param fileChannel
         *            The file channel to which to write.
         * @param dst
         *            The buffer into which bytes are transferred.
         * @param position
         *            The file position at which the transfer is to begin, must
         *            be non-negative.
         * @return The number of bytes read, possibly zero, or -1 if the given
         *         position is greater than or equal to the file's current size.
         * @throws IOException
         *             If an I/O error occurs.
         */
        public int read(FileChannel fileChannel, ByteBuffer dst, long position) throws IOException
        {
            return fileChannel.read(dst, position);
        }

        /**
         * Returns the current size of the file channel's file.
         * 
         * @param fileChannel
         *            The file channel to size.
         * @return The current size of this channel's file, measured in bytes.
         * @throws IOException
         *             If an I/O error occurs.
         */
        public long size(FileChannel fileChannel) throws IOException
        {
            return fileChannel.size();
        }
        
        public FileChannel truncate(FileChannel fileChannel, long size) throws IOException
        {
            return fileChannel.truncate(size);
        }

        /**
         * Forces any updates to the file channel's file to be written to the
         * storage device that contains it.
         * 
         * @param fileChannel
         *            The file channel to flush.
         * @throws IOException
         *             If an I/O error occurs.
         */
        public void force(FileChannel fileChannel) throws IOException
        {
            fileChannel.force(true);
        }

        /**
         * Closes the file channel.
         * 
         * @param fileChannel
         *            The file channel to close.
         * @throws IOException
         *             If an I/O error occurs.
         */
        public void close(FileChannel fileChannel) throws IOException
        {
            fileChannel.close();
        }
    }
 
    private final static class Pager
    {
        private final Checksum checksum;

        private final FileChannel fileChannel;

        private final Disk disk;

        private final int pageSize;
        
        private final Header header;

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

        private final ReferenceQueue<RawPage> queue;

        public final File file;

        public final Map<URI, Long> mapOfStaticPages;

        private final int alignment;

        public final BySizeTable pagesBySize;

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

        private final Boundary dataBoundary;
        
        private final Boundary interimBoundary;
            
        private final MoveList listOfMoves;
        
        private final PositionSet setOfJournalHeaders;
        
        private final SortedSet<Long> setOfAddressPages;
        
        private final Set<Long> setOfReturningAddressPages;
        
        public Pager(File file, FileChannel fileChannel, Disk disk, Header header, Map<URI, Long> mapOfStaticPages, SortedSet<Long> setOfAddressPages, long interimBoundary)
        {
            this.file = file;
            this.fileChannel = fileChannel;
            this.disk = disk;
            this.header = header;
            this.alignment = header.getAlignment();
            this.pageSize = header.getPageSize();
            this.dataBoundary = new Boundary(pageSize, header.getDataBoundary());
            this.interimBoundary = new Boundary(pageSize, interimBoundary);
            this.checksum = new Adler32();
            this.mapOfPagesByPosition = new HashMap<Long, PageReference>();
            this.pagesBySize = new BySizeTable(pageSize, alignment);
            this.mapOfStaticPages = mapOfStaticPages;
            this.setOfFreeUserPages = new TreeSet<Long>();
            this.setOfFreeInterimPages = new TreeSet<Long>(new Reverse<Long>());
            this.queue = new ReferenceQueue<RawPage>();
            this.moveListLock = new ReentrantReadWriteLock();
            this.compactLock = new ReentrantReadWriteLock();
            this.expandLock = new ReentrantLock();
            this.listOfMoves = new MoveList();
            this.setOfJournalHeaders = new PositionSet(FILE_HEADER_SIZE, header.getInternalJournalCount());
            this.setOfAddressPages = setOfAddressPages;
            this.setOfReturningAddressPages = new HashSet<Long>();
        }
        
        // FIXME Me misnomer. It should be getAddressStart.
        public long getFirstAddressPageStart()
        {
            return header.getFirstAddressPageStart();
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
        
        public Boundary getDataBoundary()
        {
            return dataBoundary;
        }
        
        public Boundary getInterimBoundary()
        {
            return interimBoundary;
        }
        
        public MoveList getMoveList()
        {
            return listOfMoves;
        }
        
        public int getAlignment()
        {
            return alignment;
        }
        
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
        
        public boolean recover(Recovery recovery, long position, Medic medic)
        {
            RawPage rawPage = getPageByPosition(position);
            if (rawPage == null)
            {
                rawPage = new RawPage(this, position);
                Page page = medic.recover(rawPage, recovery);
                if (page != null)
                {
                    rawPage.setPage(page);
                    addPageByPosition(rawPage);
                    return true;
                }
                return false;
            }
            return true;
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
        
        public void close()
        {
            getCompactLock().writeLock().lock();
            try
            {
                int size = 0;
                
                size += COUNT_SIZE + setOfAddressPages.size() * POSITION_SIZE;
                size += COUNT_SIZE + (setOfFreeUserPages.size() + pagesBySize.getSize()) * POSITION_SIZE;
                
                ByteBuffer reopen = ByteBuffer.allocateDirect(size);
                
                reopen.putInt(setOfAddressPages.size());
                for (long position: setOfAddressPages)
                {
                    reopen.putLong(position);
                }
               
                reopen.putInt(setOfFreeUserPages.size() + pagesBySize.getSize());
                for (long position: setOfFreeUserPages)
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
                    throw new Danger(ERROR_IO_WRITE, e);
                }
                
                try
                {
                    disk.truncate(fileChannel, getInterimBoundary().getPosition() + reopen.capacity());
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_TRUNCATE, e);
                }
                
                header.setDataBoundary(dataBoundary.getPosition());
                header.setOpenBoundary(getInterimBoundary().getPosition());

                header.setShutdown(SOFT_SHUTDOWN);
                try
                {
                    header.write(disk, fileChannel);
                    disk.close(fileChannel);
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_CLOSE, e);
                }
            }
            finally
            {
                getCompactLock().writeLock().unlock();
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

        private long fromWilderness()
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);

            bytes.getLong(); // Checksum.
            bytes.putInt(-1); // Is system page.

            bytes.clear();

            checksum(checksum, bytes);

            long position;

            synchronized (disk)
            {
                try
                {
                    position = disk.size(fileChannel);
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_SIZE, e);
                }

                try
                {
                    disk.write(fileChannel, bytes, position);
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_WRITE, e);
                }

                try
                {
                    if (disk.size(fileChannel) % 1024 != 0)
                    {
                        throw new Danger(ERROR_FILE_SIZE);
                    }
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_SIZE, e);
                }
            }

            return position;
        }

        private RawPage getPageByPosition(long position)
        {
            RawPage page = null;
            Long boxPosition = new Long(position);
            PageReference chunkReference = (PageReference) mapOfPagesByPosition.get(boxPosition);
            if (chunkReference != null)
            {
                page = (RawPage) chunkReference.get();
            }
            return page;
        }

        private RawPage removePageByPosition(long position)
        {
            PageReference existing = (PageReference) mapOfPagesByPosition.get(new Long(position));
            RawPage p = null;
            if (existing != null)
            {
                p = existing.get();
                existing.enqueue();
                collect();
            }
            return p;
        }

        private void addPageByPosition(RawPage page)
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
            RawPage position = new RawPage(this, value);
            page.create(position, dirtyPages);

            synchronized (mapOfPagesByPosition)
            {
                assert getPageByPosition(value) == null;
                addPageByPosition(position);
            }

            return (P) position.getPage();
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
        
        public AddressPage tryGetAddressPage(long lastSelected)
        {
            synchronized (setOfAddressPages)
            {
                long position = 0L;
                if (setOfAddressPages.size() == 0 && setOfReturningAddressPages.size() == 0)
                {
                    final PageRecorder pageRecorder = new PageRecorder();
                    final MoveList listOfMoves = new MoveList(pageRecorder, getMoveList());
                    Mutator mutator = listOfMoves.mutate(new Returnable<Mutator>()
                    {
                        public Mutator run()
                        {
                            MoveNode moveNode = new MoveNode(new Move(0, 0));
                            DirtyPageMap dirtyPages = new DirtyPageMap(Pager.this, 16);
                            Journal journal = new Journal(Pager.this, pageRecorder, moveNode, dirtyPages);
                            return new Mutator(Pager.this, listOfMoves, pageRecorder, journal, moveNode, dirtyPages);
                        }
                    });
                    position = mutator.newAddressPage();
                    // FIXME Here is something that hasn't been dealt with yet.
                    mutator.commit();
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
                AddressPage addressPage = getPage(position, new AddressPage());
                if (addressPage.getFreeCount() > 1)
                {
                    setOfReturningAddressPages.add(position);
                }
                return addressPage;
            }
        }
        
        public void returnUserPage(DataPage blockPage)
        {
            // FIXME Is it not the case that the block changes can mutated
            // by virtue of a deallocation operation?
            if (blockPage.getCount() == 0)
            {
                synchronized (setOfFreeUserPages)
                {
                    setOfFreeUserPages.add(blockPage.getRawPage().getPosition());
                }
            }
            else if (blockPage.getRemaining() > getAlignment())
            {
                pagesBySize.add(blockPage);
            }
        }
        
        public void returnAddressPage(AddressPage addressPage)
        {
            if (addressPage.getFreeCount() != 0)
            {
                long position = addressPage.getRawPage().getPosition();
                synchronized (setOfAddressPages)
                {
                    setOfReturningAddressPages.remove(position);
                    setOfAddressPages.add(position);
                    setOfAddressPages.notifyAll();
                }
            }
        }

        // FIXME Rename everything in this method.
        // FIXME Document this method.
        @SuppressWarnings("unchecked")
        public <P extends Page> P getPage(long value, P page)
        {
            RawPage position = null;
            synchronized (mapOfPagesByPosition)
            {
                value = (long) Math.floor(value - (value % pageSize));
                position = getPageByPosition(value);
                if (position == null)
                {
                    position = new RawPage(this, value);
                    page.load(position);
                    addPageByPosition(position);
                }
            }
            // FIXME Am I down grading when looking for RelocatablePage?
            // FIXME Shouldn't this be assignable from?
            // FIXME Assert that if not assignable one way, it is assignable the other way.
            synchronized (position)
            {
                if (!position.getPage().getClass().equals(page.getClass()))
                {
                    page.load(position);
                }
            }
            return castPage(position.getPage(), page);
        }
        
        @SuppressWarnings("unchecked")
        private <P extends Page> P castPage(Page page, P subtype)
        {
            return (P) page;
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

            long position = popFreeInterimPage();

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
                    setOfFreeInterimPages.remove(position);
                    return true;
                }
            }
            return false;
        }

        public boolean removeDataPageIfFree(long position)
        {
            synchronized (setOfFreeUserPages)
            {
                if (setOfFreeUserPages.contains(position))
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
                RawPage position = removePageByPosition(head.getMove().getFrom());
                if (position != null)
                {
                    assert head.getMove().getTo() == position.getPosition();
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

    /**
     * A representation of the basic page attributes of position and byte buffer
     * associated with an application of the raw page implemented by {@link
     * Pack.Page}. The raw page is always the page size of the Pack file and is
     * always aligned to a page boundary. 
     * <p>
     * The raw page  is participant in a strategy design pattern, where the
     * content of the bytes at the position are interpreted by an associated
     * implementation of {@link Pack.Page}.  
     * <p>
     * The raw page maintains soft reference to a byte buffer of the bytes of
     * the raw page and is itself soft referenced within the map of pages by
     * position within {@link Pack.Pager}.
     * <p>
     * The soft references allow both the raw page and the byte buffer to be
     * reclaimed and reclaimed separately.
     * <p>
     * All pages write their changes out to the byte buffer. We hold onto a hard
     * referenced to dirty raw pages and we hold onto a hard reference to their
     * associated byte buffers in a dirty page map. The dirty page map will
     * write the dirty pages to file. Once the raw page is written to disk we
     * let go of the hard reference to the raw page and byte buffer in the raw
     * page map. It can be collected. The separate soft references only benefit
     * the data page which maintains a concurrency lock in the data page object.
     * See {@link Pack.DataPage} for more details on the separate soft
     * references.
     */
    private static final class RawPage extends Regional
    {
        /**
         * The pager that manages this raw page.
         */
        private final Pager pager;

        /**
         * A reference to a class that implements a specific application of a
         * raw page, the strategy for this page according to the strategy design
         * pattern.
         */
        private Page page;

        /**
         * The page size boundary aligned position of the page in file.
         */
        private long position;

        /**
         * A reclaimable reference to a byte buffer of the contents of the raw
         * page. The byte buffer is lazy loaded when needed and can be reclaimed
         * whenever there is no hard reference to the byte buffer. We keep a
         * hard reference to the byte buffer using a dirty page map, which
         * gathers dirty buffers and writes them out in a batch.
         */
        private Reference<ByteBuffer> byteBufferReference;

        /**
         * Create a raw page at the specified position associated with the
         * specified pager. The byte buffer is not loaded until the {@link
         * #getByteBuffer getByteBuffer} method is called.
         * 
         * @param pager
         *            The pager that manages this raw page.
         * @param position
         *            The position of the page.
         */
        public RawPage(Pager pager, long position)
        {
            super(position);
            this.pager = pager;
        }

        /**
         * Get the pager that manages this raw page.
         *
         * @return The pager.
         */
        public Pager getPager()
        {
            return pager;
        }

        /**
         * Set the class that implements a specific application of a raw page,
         * the strategy for this page according to the strategy design pattern.
         *
         * @param page The specific application of this raw page.
         */
        public void setPage(Page page)
        {
            this.page = page;
        }

        /**
         * Get the class that implements a specific application of a raw page,
         * the strategy for this page according to the strategy design pattern.
         *
         * @return The specific application of this raw page.
         */
        public Page getPage()
        {
            return page;
        }

        /**
         * Load the byte buffer from the file channel of the pager.
         */
        private ByteBuffer load()
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
                pager.getDisk().read(pager.getFileChannel(), bytes, position);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_READ, e);
            }
            bytes.clear();

            return bytes;
        }

        /**
         * Returns the softly referenced byte buffer of the content of this raw
         * page.
         * <p>
         * The byte buffer is lazily initialized, so that the byte buffer is
         * read on the first call to this method.
         * <p>
         * The byte buffer is also softly referenced so this method checks to
         * see if the byte buffer has been reclaimed and reads in a new byte
         * buffer it has been reclaimed.
         * <p>
         * Whenever we write to the byte buffer we hold onto a hard
         * reference to the byte buffer until the byte buffer is written back to
         * file. We keep a hard refernce to the raw page and the byte buffer
         * returned by this method in the dirty page map. The dirty page map
         * will flush the page.
         *
         * @return A byte buffer of the contents of this page.
         */
        public synchronized ByteBuffer getByteBuffer()
        {
            ByteBuffer bytes = null;

            if (byteBufferReference == null)
            {
                bytes = load();
                byteBufferReference = new WeakReference<ByteBuffer>(bytes);
            }
            else
            {
                bytes = (ByteBuffer) byteBufferReference.get();
                if (bytes == null)
                {
                    bytes = load();
                    byteBufferReference = new WeakReference<ByteBuffer>(bytes);
                }
            }

            return bytes;
        }
    }

    /**
     * The <code>Page</code> interface is a specialized application of a
     * {@link Pack.RawPage}. It is the strategy participant in the strategy
     * design pattern. The raw page contains only the basic position and byte
     * buffer page attributes. A <code>Page</code> implementation interprets
     * the raw page as a specific page type.
     * <h4>Frequent Instanciation and Two-Step Initialization</h4>
     * <p>
     * When we request a page from the the pager, we provide and implementation
     * of <code>Page</code>. If the page exists in the pager, the pager will
     * ensure that the <code>Page</code> is assignable to the class of the
     * specified <code>Page</code>. If the page does not exist in the pager,
     * a new <code>RawPage</code> is created and the specified
     * <code>Page</code> is associated with the <code>RawPage</code>.
     * <p>
     * Thus, we create a lot of <code>Page</code> instances that are not
     * actually initialized. They are used as flags when requesting a page from
     * the <code>Pager</code>. The <code>Pager</code> will use them as
     * actual <code>Page</code> implementations only if page is not available
     * in the <code>Pager</code> or if the associated <code>Page</code>
     * implementation of the page in the <code>Pager</code> is a superclass of
     * the desired <code>Page</code> implementation.
     * <p>
     * All <code>Page</code> implementations perform minimal initialization at
     * construction. They are actually initialized by a call to either {@link
     * #create create} or {@link #load load}.
     * <h4>Circular RawPage and Page Reference</h4>
     * <p>
     * The circular reference between <code>Page</code> and
     * <code>RawPage</code> is necessary. <code>RawPage</code> is used as
     * the mutex for changes to the underlying byte buffer as well as to the
     * data members of the <code>Page</code> itself. All the <code>Page</code>
     * implementations use the associated <code>RawPage</code> as the lock
     * object in synchronized blocks as opposed to using synchronized methods on
     * the <code>Page</code> itself.
     * <p>
     * The pager keeps a map by position of the <code>RawPage</code> object an
     * not the <code>Page</code> objects, because the <code>Page</code>
     * associated with a <code>RawPage</code> can change. Specifically, the
     * <code>Page</code> associated with a <code>RawPage</code> can be
     * promoted from a <code>RelocatablePage</code> to a subclass of
     * <code>RelocatablePage</code>
     * <p>
     * The circular reference is necessary. The <code>Page</code>
     * implementation will need read and write the <code>RawPage</code>.The
     * <code>RawPage</code> is the constant that is mapped to the position in
     * the <code>Pager</code>. The <code>RawPage</code> is the lock on the
     * region in the file and it's associated <code>Page</code> can change.
     * 
     * @see Pack.Pager#getPage
     */
    private interface Page
    {
        /**
         * Return the underlying raw page associated with this page.
         * 
         * @return The raw page.
         */
        public RawPage getRawPage();

        /**
         * Initialize the raw page to the specific interpretation implemented by
         * this page. This method is called from within the <code>Pager</code>
         * when a new raw page is allocated. The specified raw page will
         * subsequently be returned by {@link #getRawPage getRawPage}.
         * 
         * @param rawPage
         *            The raw page.
         * @param dirtyPages
         *            The collection of dirty pages.
         * @see Pack.Pager#getPage
         */
        public void create(RawPage rawPage, DirtyPageMap dirtyPages);

        /**
         * Load this specific interpretation from the specified the raw page.
         * This method is called from within the {@link Pack.Pager#getPage
         * getPage} method of the <code>Pager</code> when a page is loaded
         * from the file. The specified <code>RawPage</code> will be
         * subsequently returned by {@link #getRawPage getRawPage}.
         * 
         * @param rawPage
         *            The raw page.
         * @see Pack.Pager#getPage
         */
        public void load(RawPage rawPage);
    }

    /**
     * Interprets a {@link RawPage} as a position as an array of addresses that
     * reference specific positions in the data region of the file. A user
     * address references a position in an address page that contains a long
     * value indicating the position of the user data in the data region of the
     * file.
     * <p>
     * The address itself is a long value indicating the actual position of the
     * user data file position long value in the address page. It is an
     * indirection. To find the position of a user data block, we read the long
     * value at the position indicated by the address to find the page that
     * contains the user block. We then scan the data page for the block that
     * contains the address in its address back-reference.
     * <p>
     * Unused addresses are indicated by a zero data position value. If an
     * address is in use, there will be a non-zero position value in the slot.
     * <p>
     * When we allocate a new block, because of isolation, we cannot write out
     * the address of the new data block until we are playing back a flushed
     * journal. Thus, during the mutation phase of a mutation, we need to
     * reserve a free address. Reservations are tracked in an interim
     * reservation page defined by {@link Pack.ReservationPage}. The
     * reservation page says which of the free addresses are reserved.
     * <p>
     * The associate reservation page is allocated as needed. If there is no
     * associated reservation page, then none of the free addresses are
     * reserved.
     */
    private static final class AddressPage
    implements Page, Medic
    {
        private RawPage rawPage;
        
        private int freeCount;
        
        /**
         * Construct an uninitialized address page that is then initialized by
         * calling the {@link #create create} or {@link #load load} methods. The
         * default constructor creates an empty address page that must be
         * initialized before use.
         * <p>
         * All of the page classes have default constructors. This constructor
         * is called by clients of the <code>Pager</code> when requesting pages
         * or creating new pages. An uninitialized page of the expected Java
         * class of page is given to the <code>Pager</code>. If the page does
         * not exist, the newly created page is used, if not is ignored and
         * garbage collected.
         *
         * @see com.agtrz.pack.Pack.Pager#getPage
         */
        public AddressPage()
        {
        }

        public void load(RawPage rawPage)
        {
            ByteBuffer bytes = rawPage.getByteBuffer();

            bytes.clear();
            
            bytes.position(getHeaderOffset(rawPage));
 
            bytes.getLong();
            
            while (bytes.remaining() > Long.SIZE / Byte.SIZE)
            {
                long position = bytes.getLong();
                if (position == 0L)
                {
                    freeCount++;
                }
            }

            rawPage.setPage(this);
            this.rawPage = rawPage;
        }

        public void create(RawPage rawPage, DirtyPageMap dirtyPages)
        {
            ByteBuffer bytes = rawPage.getByteBuffer();

            bytes.clear();
            
            bytes.putLong(0L);
            
            while (bytes.remaining() > Long.SIZE / Byte.SIZE)
            {
                bytes.putLong(0L);
                freeCount++;
            }
 
            dirtyPages.add(rawPage);

            this.rawPage = rawPage;
            rawPage.setPage(this);
        }
        
        private boolean verifyChecksum(RawPage rawPage, Recovery recovery)
        {
            Checksum checksum = recovery.getChecksum();

            ByteBuffer bytes = rawPage.getByteBuffer();
            bytes.clear();
            bytes.position(ADDRESS_PAGE_HEADER_SIZE);
            while (bytes.remaining() >= Long.SIZE / Byte.SIZE)
            {
                checksum.update(bytes.get());
            }
            
            if (bytes.getLong(0) != checksum.getValue())
            {
                recovery.badAddressChecksum(rawPage.getPosition());
                return false;
            }
            
            return true;
        }
        
        private boolean verifyAddresses(RawPage rawPage, Recovery recovery)
        {
            boolean copacetic = true;

            ByteBuffer bytes = rawPage.getByteBuffer();

            bytes.clear();
            bytes.position(ADDRESS_PAGE_HEADER_SIZE);
            
            while (bytes.remaining() >= Long.SIZE / Byte.SIZE)
            {
                long address = rawPage.getPosition() + bytes.position();
                long position = bytes.getLong();
                if (position < rawPage.getPager().getPageSize())
                {
                    copacetic = false;
                }
                else if (position > recovery.getFileSize())
                {
                    copacetic = false;
                }
                else if (rawPage.getPager().recover(recovery, position, new DataPage(false)))
                {
                    DataPage dataPage = rawPage.getPager().getPage(position, new DataPage(false));
                    if (!dataPage.verify(address, position))
                    {
                        copacetic = false;
                    }
                }
            }
            
            return copacetic;
        }

        public Page recover(RawPage rawPage, Recovery recovery)
        {
            verifyChecksum(rawPage, recovery);
            
            if (recovery.getRegion() == ADDRESS_REGION)
            {
                if (verifyAddresses(rawPage, recovery))
                {
                    load(rawPage);
                    return this;
                }
            }
            
            return null;
        }

        public int getFreeCount()
        {
            synchronized (getRawPage())
            {
                return freeCount;
            }
        }

        public RawPage getRawPage()
        {
            return rawPage;
        }
        
        private static int getHeaderOffset(RawPage rawPage)
        {
            int offset = 0;
            long first = rawPage.getPager().getFirstAddressPageStart();
            if (rawPage.getPosition() < first)
            {
                offset = (int) (first % rawPage.getPager().getPageSize());
            }
            return offset;
        }

        /**
         * Adjust the starting offset for addresses in the address page
         * accounting for the header and for the file header, if this is the
         * first address page in file.
         *
         * @return The start offset for iterating through the addresses.
         */
        private int getFirstAddressOffset()
        {
            return getHeaderOffset(getRawPage()) + ADDRESS_PAGE_HEADER_SIZE;
        }
        
        /**
         * Return the page position associated with the address.
         *
         * @return The page position associated with the address.
         */
        public long dereference(long address)
        {
            synchronized (getRawPage())
            {
                int offset = (int) (address - getRawPage().getPosition());
                long actual = getRawPage().getByteBuffer().getLong(offset);

                assert actual != 0L; 

                return actual;
            }
        }

        /**
         * Reserve an available address from the address page. Reserving an
         * address requires marking it as reserved on an associated page
         * reservation page. The parallel page is necessary because we cannot
         * change the zero state of the address until the page is committed.
         * <p>
         * The reservation page is tracked with the dirty page map. It can be
         * released after the dirty page map flushes the reservation page to
         * disk.
         * 
         * @param dirtyPages
         *            The dirty page map.
         * @return An reserved address or 0 if none are available.
         */
        public long reserve(DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                // Get the page buffer.
                
                ByteBuffer bytes = getRawPage().getByteBuffer();

                // Iterate the page buffer looking for a zeroed address that has
                // not been reserved, reserving it and returning it if found.
                
                long position = getRawPage().getPosition();
                for (int i = getFirstAddressOffset(); i < bytes.capacity(); i += ADDRESS_SIZE)
                {
                    if (bytes.getLong(i) == 0L)
                    {
                        dirtyPages.add(getRawPage());
                        bytes.putLong(i, Long.MAX_VALUE);
                        getRawPage().invalidate(i, POSITION_SIZE);
                        return position + i;
                    }
                }

                // Not found.
                
                return 0L;
            }
        }

        public void set(long address, long value, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = rawPage.getByteBuffer();
                bytes.putLong((int) (address - rawPage.getPosition()), value);
                dirtyPages.add(getRawPage());
            }
        }
    }

    private static class RelocatablePage
    implements Page
    {
        private RawPage rawPage;
        
        public void create(RawPage rawPage, DirtyPageMap dirtyPages)
        {
            this.rawPage = rawPage;
            rawPage.setPage(this);
        }

        public void load(RawPage rawPage)
        {
            this.rawPage = rawPage;
            rawPage.setPage(this);
        }
        
        public RawPage getRawPage()
        {
            return rawPage;
        }

        /**
         * Relocate a page from one position to another writing it out
         * immediately. This method does not use a dirty page map, the page
         * is written immediately.
         * 
         * @param to
         *            The position where the page will be relocated.
         */
        public void relocate(long to)
        {
            // FIXME Not enough locking?
            RawPage rawPage = getRawPage();
            Pager pager = rawPage.getPager();
            ByteBuffer bytes = rawPage.getByteBuffer();
            try
            {
                pager.getDisk().write(pager.getFileChannel(), bytes, to);
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
            rawPage.setPosition(to);
        }
    }

    /**
     * An application of a raw page that manages the page a list of data blocks.
     * <h4>Duplicate Soft References</h4>
     * <p>
     * The data page is the only page that can take advantage of the duplication
     * soft references in the raw page class. The raw page holds a soft
     * refernece to the byte buffer. It is itself soft referneced by the map of
     * pages by position in the pager.
     * <p>
     * All pages write their changes out to the byte buffer. We hold onto dirty
     * raw pages in a dirty page map. Once the raw page is written to disk we
     * let go of the hard reference to the raw page in the raw page map. It can
     * be collected.
     * <p>
     * The data page also contains a lock that keeps another mutator from
     * writing to it when it is being vacuumed. The lock is based on the
     * <code>wait</code> and <code>notify</code> methods of the data page
     * object. The byte buffer may be flused to disk, but a data page waiting
     * to be vacuumed still needs to be held in memory because of the lock.
     * <h4>Two-Stage Vacuum</h4>
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
     * <h4>Deadlock</h4>
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
     * <p>
     * FIXME RENAME BlockPage
     */
    private static final class DataPage
    extends RelocatablePage implements Medic
    {
        private int remaining;

        private int count;

        private boolean interim;
        
        /**
         * True if the page is in the midst of a vacuum and should not
         * be written to.
         */
        private boolean mirrored;
        
        public DataPage(boolean interim)
        {
            this.mirrored = false;
            this.interim = interim;
        }
        
        private int getDiskCount()
        {
            if (interim)
            {
                return count;
            }
            return count == 0 ? Integer.MIN_VALUE : -count;
        }

        public void create(RawPage rawPage, DirtyPageMap dirtyPages)
        {
            super.create(rawPage, dirtyPages);
            
            this.count = 0;
            this.remaining = rawPage.getPager().getPageSize() - DATA_PAGE_HEADER_SIZE;
            
            ByteBuffer bytes = rawPage.getByteBuffer();
            
            bytes.putInt(CHECKSUM_SIZE, getDiskCount());
            
            dirtyPages.add(rawPage);
        }

        public void load(RawPage rawPage)
        {    
            super.load(rawPage);

            ByteBuffer bytes = rawPage.getByteBuffer();
            int count = bytes.getInt();

            if ((count & COUNT_MASK) != 0)
            {
                assert !interim;
                count = count == Integer.MIN_VALUE ? 0 : count & ~COUNT_MASK;
            }
            
            this.remaining = getRemaining(count, bytes);
        }
        
        private boolean verifyChecksum(RawPage rawPage, Recovery recovery)
        {
            Checksum checksum = recovery.getChecksum();
            ByteBuffer bytes = rawPage.getByteBuffer();
            
            bytes.clear();
            bytes.position(CHECKSUM_SIZE);
            
            for (int i = 0; i < Integer.SIZE / Byte.SIZE; i++)
            {
                checksum.update(bytes.get());
            }
            
            int count = bytes.getInt(CHECKSUM_SIZE);
//            boolean isInterim = (count & 0XA0000000) != 0;
            count = (count & ~0xA0000000);
            
            int block = 0;
            while (block < count)
            {
                int size = bytes.getInt();
                
                if (Math.abs(size) > bytes.remaining())
                {
                    recovery.corruptDataPage(rawPage.getPosition());
                    return false;
                }
                
                if (size > 0)
                {
                    block++;
                    for (int i = 0; i < Math.abs(size); i++)
                    {
                        checksum.update(bytes.get());
                    }
                }
                else
                {
                    bytes.position(bytes.position() + Math.abs(size));
                }
            }
            
            if (checksum.getValue() != bytes.getLong(0))
            {
                recovery.badDataChecksum(rawPage.getPosition());
                return false;
            }
            
            return true;
        }
        
        public Page recover(RawPage rawPage, Recovery recovery)
        {
            if (verifyChecksum(rawPage, recovery))
            {
                load(rawPage);
                return this;
            }
            
            return null;
        }

        private static int getRemaining(int count, ByteBuffer bytes)
        {
            bytes.clear();
            for (int i = 0; i < count; i++)
            {
                bytes.getLong();
                int size = bytes.getInt();
                int advance = Math.abs(size) > bytes.remaining() ? bytes.remaining() : Math.abs(size);
                bytes.position(bytes.position() + advance);
            }
            return bytes.remaining();
        }

        public int getCount()
        {
            synchronized (getRawPage())
            {
                return count;
            }
        }

        public int getRemaining()
        {
            synchronized (getRawPage())
            {
                return remaining;
            }
        }

        public void reset(short type)
        {
            synchronized (getRawPage())
            {
                this.count = 0;
                this.remaining = getRemaining(count, getRawPage().getByteBuffer());
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
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
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
            synchronized (getRawPage())
            {
                assert !mirrored;

                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
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
                            vacuumPage = pager.newSystemPage(new DataPage(true), dirtyPages);

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
                    journal.write(new AddVacuum(vacuumPage.getRawPage().getPosition()));
                }
                return mirrored;
            }
        }

        public long allocate(long address, int length, DirtyPageMap dirtyPages)
        {
            long position = 0L;
            synchronized (getRawPage())
            {
                assert ! mirrored;

                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
                int block = 0;
                while (block != count && position == 0L)
                {
                    int offset = bytes.position();
                    int size = getSize(bytes);
                    if (size > 0 && bytes.getLong(bytes.position()) == address)
                    {
                        position = getRawPage().getPosition() + offset;
                    }
                    else
                    {
                        bytes.position(bytes.position() + Math.abs(size));
                    }
                }

                if (position == 0L)
                {
                    position = getRawPage().getPosition() + bytes.position();

                    bytes.putInt(length);
                    bytes.putLong(address);

                    count++;
                    remaining -= length;

                    bytes.clear();
                    bytes.getLong();
                    bytes.putInt(count);
                }
            }

            // FIXME Byte buffer can get reclaimed.
            dirtyPages.add(getRawPage());

            return position;
        }

        // FIXME This is the method I should use, right?
        public boolean write(long address, ByteBuffer data, DirtyPageMap pages)
        {
            synchronized (getRawPage())
            {
                // FIXME I forgot what this was about.
                while (mirrored)
                {
                    try
                    {
                        getRawPage().wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
                if (seek(bytes, address))
                {
                    int size = getSize(bytes);
                    bytes.putLong(address);
                    bytes.limit(size - POSITION_SIZE);
                    bytes.put(data);
                    pages.add(getRawPage());
                    return true;
                }
                return false;
            }
        }

        public void write(long position, long address, ByteBuffer data, DirtyPageMap dirtyPages)
        {
            // FIXME Seek.
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getRawPage().getByteBuffer();
                bytes.clear();
                bytes.position((int) (position - getRawPage().getPosition()));
                int size = bytes.getInt();
                if (address != bytes.getLong())
                {
                    throw new IllegalStateException();
                }
                bytes.limit(bytes.position() + (size - BLOCK_HEADER_SIZE));
                bytes.put(data);
                dirtyPages.add(getRawPage());
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
            return (int) (position - getRawPage().getPosition());
        }

        public boolean free(long address, DirtyPageMap pages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
                if (seek(bytes, address))
                {
                    int offset = bytes.position();

                    int size = getSize(bytes);
                    if (size > 0)
                    {
                        size = -size;
                    }
                    bytes.putInt(offset, size);

                    pages.add(getRawPage());
                }
                return true;
            }
        }

        public void compact()
        {
            throw new UnsupportedOperationException();
        }
        
        public boolean verify(long address, long position)
        {
            throw new UnsupportedOperationException();
        }
        
        public void commit(long address, ByteBuffer block, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                AddressPage addressPage = getRawPage().getPager().getPage(address, new AddressPage());
                synchronized (addressPage.getRawPage())
                {
                    ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
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
                        
                        addressPage.set(address, getRawPage().getPosition() + offset, dirtyPages);
                    }
                }
            }
            // FIXME No hard reference to bytes.
            dirtyPages.add(getRawPage());
        }

        public void commit(DataPage dataPage, DirtyPageMap dirtyPages)
        {
            // FIXME Locking a lot. Going to deadlock?
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
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
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getBlockRange(getRawPage().getByteBuffer());
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

        public void create(RawPage position, DirtyPageMap dirtyPages)
        {
            super.create(position, dirtyPages);

            ByteBuffer bytes = getRawPage().getByteBuffer();
            
            bytes.clear();
            bytes.getLong();
            bytes.putInt(-1);

            getRawPage().setPage(this);
            
            this.offset = JOURNAL_PAGE_HEADER_SIZE;
        }

        public void load(RawPage position)
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
            ByteBuffer bytes = getRawPage().getByteBuffer();
            
            bytes.clear();
            bytes.position(offset);

            return bytes;
        }

        public boolean write(Operation operation, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getByteBuffer();

                if (operation.length() + NEXT_PAGE_SIZE < bytes.remaining())
                {
                    operation.write(bytes);
                    offset = bytes.position();
                    dirtyPages.add(getRawPage());
                    return true;
                }
                
                return false;
            }
        }

        public long getJournalPosition()
        {
            synchronized (getRawPage())
            {
                return getRawPage().getPosition() + offset;
            }
        }

        public void seek(long position)
        {
            synchronized (getRawPage())
            {
                this.offset = (int) (position - getRawPage().getPosition());
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
    extends WeakReference<RawPage>
    {
        private final Long position;

        public PageReference(RawPage page, ReferenceQueue<RawPage> queue)
        {
            super(page, queue);
            this.position = new Long(page.getPosition());
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
                listOfListsOfSizes.get(aligned / alignment).addFirst(dataPage.getRawPage().getPosition());
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

    public final static class DirtyPageMap
    {
        private final Pager pager;

        private final Map<Long, RawPage> mapOfPages;

        private final Map<Long, ByteBuffer> mapOfByteBuffers;

        private final int capacity;
        
        public DirtyPageMap(Pager pager, int capacity)
        {
            this.pager = pager;
            this.mapOfPages = new HashMap<Long, RawPage>();
            this.mapOfByteBuffers = new HashMap<Long, ByteBuffer>();
            this.capacity = capacity;
        }
        
        public void add(RawPage page)
        {
            mapOfPages.put(page.getPosition(), page);
            mapOfByteBuffers.put(page.getPosition(), page.getByteBuffer());
        }
        
        public void flushIfAtCapacity()
        {
            if (mapOfPages.size() > capacity)
            {
                flush();
            }
        }
        
        public void flush(Pointer pointer)
        {
            flush();

            synchronized (pointer.getMutex())
            {
                ByteBuffer bytes = pointer.getByteBuffer();
                bytes.clear();
                
                try
                {
                    pager.getDisk().write(pager.getFileChannel(), bytes, pointer.getPosition());
                }
                catch (IOException e)
                {
                    throw new Danger(ERROR_IO_WRITE, e);
                }
            }
        }

        public void flush()
        {
            for (RawPage rawPage: mapOfPages.values())
            {
                synchronized (rawPage)
                {
                    try
                    {
                        rawPage.write(pager.getDisk(), pager.getFileChannel());
                    }
                    catch (IOException e)
                    {
                        throw new Danger(ERROR_IO_WRITE, e);
                    }
                }
            }
            mapOfPages.clear();
            mapOfByteBuffers.clear();
        }

        public void commit(ByteBuffer journal, long position)
        {
            flush();
            Disk disk = pager.getDisk();
            FileChannel fileChannel = pager.getFileChannel();
            try
            {
                disk.write(fileChannel, journal, position);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_WRITE, e);
            }
            try
            {
                disk.force(fileChannel);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_FORCE, e);
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
                DataPage dataPage = player.getPager().getPage(position, new DataPage(false));
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
            DataPage toDataPage = pager.getPage(toPosition, new DataPage(false));
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
            DataPage dataPage = pager.getPage(referenced, new DataPage(false));
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
            DataPage interimPage = player.getPager().getPage(interim, new DataPage(true));
            DataPage dataPage = player.getPager().getPage(data, new DataPage(false));
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
            this.pageRecorder.getInterimPageSet().add(journalPage.getRawPage().getPosition());
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
            DataPage dataPage = pager.getPage(position, new DataPage(false));
            dataPage.write(position, address, bytes, dirtyPages);
        }

        public void write(long position, long address, ByteBuffer bytes)
        {
            DataPage dataPage = pager.getPage(position, new DataPage(false));
            dataPage.write(position, address, bytes, dirtyPages);
        }

        public ByteBuffer read(long position, long address)
        {
            DataPage dataPage = pager.getPage(position, new DataPage(false));
            return dataPage.read(position, address);
        }

        public void write(Operation operation)
        {
            if (!journalPage.write(operation, dirtyPages))
            {
                JournalPage nextJournalPage = pager.newSystemPage(new JournalPage(), dirtyPages);
                journalPage.write(new NextOperation(nextJournalPage.getJournalPosition()), dirtyPages);
                journalPage = nextJournalPage;
                pageRecorder.getInterimPageSet().add(journalPage.getRawPage().getPosition());
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
        
        private final SortedMap<Long, MovablePosition> mapOfMovePages;
        
        private final MoveNode firstMoveNode;

        private MoveNode moveNode;
        
        public CommitMoveRecorder(PageRecorder pageRecorder, Journal journal, MoveNode moveNode)
        {
            this.pageRecorder = pageRecorder;
            this.journal = journal;
            this.setOfDataPages = new HashSet<Long>();
            this.mapOfVacuums = new TreeMap<Long, MovablePosition>();
            this.mapOfPages = new TreeMap<Long, MovablePosition>();
            this.mapOfMovePages = new TreeMap<Long, MovablePosition>();
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
        
        public SortedMap<Long, MovablePosition> getMovePageMap()
        {
            return mapOfMovePages;
        }
        
        public PageRecorder getPageRecorder()
        {
            return pageRecorder;
        }
    }
    
    /**
     * A superfluous little class.
     */
    private final static class Boundary
    {
        private final int pageSize;
        
        private long position;
        
        public Boundary(int pageSize, long position)
        {
            this.pageSize = pageSize;
            this.position = position;
        }
        
        public long getPosition()
        {
            return position;
        }
        
        public void increment()
        {
            position += pageSize;
        }
        
        public void decrement()
        {
            position -= pageSize;
        }
    }

    public final static class Mutator
    {
        private final Pager pager;
        
        private final Journal journal;

        private final BySizeTable allocPagesBySize;
        
        private final BySizeTable writePagesBySize;

        private final Map<Long, MovablePosition> mapOfAddresses;

        private final DirtyPageMap dirtyPages;
        
        private MutateMoveRecorder moveRecorder;
        
        private final PageRecorder pageRecorder;
        
        private final MoveList listOfMoves;
        
        private long lastPointerPage;

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
            AddressPage addressPage = null;
            long address = 0L;
            addressPage = pager.getAddressPage(lastPointerPage);
            try
            {
                address = addressPage.reserve(dirtyPages);
            }
            finally
            {
                pager.returnAddressPage(addressPage);
            }
            
            return allocate(address, blockSize);
        }
        
        private long allocate(final long address, int blockSize)
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
                        throw new UnsupportedOperationException();
                    }
                    
                    // If we already have a wilderness data page that will fit
                    // the block, use that page. Otherwise, allocate a new
                    // wilderness data page for allocation.
                    
                    DataPage allocPage = null;
                    long alloc = allocPagesBySize.bestFit(fullSize);
                    if (alloc == 0L)
                    {
                        allocPage = pager.newSystemPage(new DataPage(true), dirtyPages);
                        moveRecorder.getPageRecorder().getAllocationPageSet().add(allocPage.getRawPage().getPosition());
                    }
                    else
                    {
                        allocPage = pager.getPage(alloc, new DataPage(true));
                    }
                    
                    // Allocate a block from the wilderness data page.
                    
                    long position = allocPage.allocate(address, fullSize, dirtyPages);
                    
                    allocPagesBySize.add(allocPage);
                    
                    mapOfAddresses.put(address, new MovablePosition(moveRecorder.getMoveNode(), position));
                    
                    return address;
                }
            });
        }

        public long newAddressPage()
        {
            pager.getCompactLock().readLock().lock();
            
            try
            {
                Set<Long> setOfPageAddress = new HashSet<Long>();
                setOfPageAddress.add(pager.getDataBoundary().getPosition());
                pager.getDataBoundary().increment();
                tryCommit(setOfPageAddress);
            }
            finally
            {
                pager.getCompactLock().readLock().unlock();
            }

            // Write the page move operation.
            return 0L;
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
                Set<Long> setOfPageAddress = Collections.emptySet();
                tryCommit(setOfPageAddress);
            }
            finally
            {
                pager.getCompactLock().readLock().unlock();
            }
        }
        
        private void tryCommit(Set<Long> setOfAddressPages)
        {
            final CommitMoveRecorder commit = new CommitMoveRecorder(pageRecorder, journal, moveRecorder.getMoveNode());

            // The set of address pages is a set of pages that need to be
            // moved to new data pages. The data boundary has already been
            // adjusted. The pages need to be moved.
            
            // We're going to create a set of address pages to move, which 
            // is separate from the full set of address pages to initialize.
            
            final Set<Long> setOfDataPages = new HashSet<Long>(setOfAddressPages);
            
            // If the new address page is in the set of free data pages, the
            // page does not have to be removed.
            if (setOfDataPages.size() != 0)
            {
                Iterator<Long> positions = setOfDataPages.iterator();
                while (positions.hasNext())
                {
                    long position = positions.next();
                    if (pager.removeInterimPageIfFree(position))
                    {
                        positions.remove();
                    }
                }
            }
            
            if (setOfDataPages.size() != 0)
            {
                
            }
            
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
                    // Write a terminate to end the playback loop. This
                    // terminate is the true end of the journal.

                    journal.write(new Terminate());

                    // Grab the current position of the journal. This is the
                    // actual start of playback.

                    long beforeVacuum = journal.getJournalPosition();
                    
                    // Create a vacuum operation for all the vacuums.

                    for (Map.Entry<Long, MovablePosition> entry: commit.getVacuumMap().entrySet())
                    {
                        // FIXME This has not run yet.
                        // FIXME By not holding onto the data page, the garbage
                        // collector can reap the raw page and we lose our
                        // mirroed flag.
                        DataPage dataPage = pager.getPage(entry.getValue().getValue(pager), new DataPage(false));
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
         * Map the pages in the set of pages to a soon to be moved interim
         * that is immediately after the data to interim page boundary. Each
         * page in the set will be mapped to a page immediately after the data
         * to interim boundary, incrementing the boundary as the page is
         * allocated.
         * <p>
         * If the interim page is in the list of free interim pages, remove
         * it. We will not lock it. No other mutator will reference a free
         * page because no other mutator is moving pages and no other mutator
         * will be using it for work space.
         * <p>
         * If the page is not in the list of free interim pages, we do have to
         * lock it.
         * 
         * @param setOfPages
         *            The set of pages that needs to be moved or copied into a
         *            page in data region of the file.
         * @param setOfMovingPages
         *            A set that will keep track of which pages this mutation
         *            references used in conjunction with the move list.
         * @param mapOfPages
         *            A map that associates one of the pages with an interim
         *            page that will be converted to a data page.
         * @param setOfInUse
         *            A set of the interim pages that need to be moved to a
         *            new interim pages as opposed to pages that were free.
         * @param setOfGahtered
         *            A set of the newly created data positions.
         */
        private void gatherPages(Set<Long> setOfPages,
            Set<Long> setOfMovingPages, Map<Long, MovablePosition> mapOfPages,
            Set<Long> setOfInUse, Set<Long> setOfGathered)
        {
            // For each page in the data page set.
            while (setOfPages.size() != 0)
            {
                // Use the page that is at the data to interim boundary.

                long position = pager.getInterimBoundary().getPosition();

                // If it is not in the set of free pages it needs to be moved,
                // so we add it to the set of in use.

                if (!pager.removeInterimPageIfFree(position))
                {
                    setOfInUse.add(position);
                }

                // Remove a page from the set of pages.

                // TODO Sorted set? Then I don't have to create an iterator.

                long from = setOfPages.iterator().next();
                setOfPages.remove(from);

                // Add the page to the set of pages used to track the pages
                // referenced in regards to the move list. We are going to move
                // this page and we are aware of this move. Negating the value
                // tells us not adjust our own move list for the first move
                // detected for this position.

                // Synapse: What if this page is already in our set?

                // This set is for data pages only. There is a separate set of
                // referenced interim pages. We're okay if we're moving our own
                // interim page.
                
                setOfMovingPages.add(-position);

                // We associate the move with a skipping movable position that
                // will skip the first move in the linked list of moves for this
                // mutator.

                mapOfPages.put(from, new SkippingMovablePosition(moveRecorder.getMoveNode(), position));

                // Add this page to the set of new data pages.
                
                setOfGathered.add(position);

                // Increment the data to interim boundary.

                pager.getInterimBoundary().increment();
            }
        }
        
        private void tryMove(MoveLatch head)
        {
            while (head != null)
            {
                RelocatablePage page = pager.getPage(head.getMove().getFrom(), new RelocatablePage());
                
                // FIXME Okay, where is there the write occurs.
                
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
                
                // FIXME Didn't this get recorded in a dirty page map?
                
                head.getLock().unlock();
                
                head = head.getNext();
            }  
        }

        private void tryMove(CommitMoveRecorder commit)
        {
            SortedSet<Long> setOfInUse = new TreeSet<Long>();
            SortedSet<Long> setOfGathered = new TreeSet<Long>();

            // Gather the interim pages that will become data pages, moving the
            // data to interim boundary.

            gatherPages(pageRecorder.getAllocationPageSet(),
                        commit.getDataPageSet(),
                        commit.getPageMap(),
                        setOfInUse,
                        setOfGathered);

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

            // At this point we have guarded these pages in this way. No one
            // else is able to move pages at all because we hold the move lock.
            // We waited for everyone else to complete committing, allocating,
            // writing, etc. by locking the move list exclusively. No one is
            // going to be interested in the pages we've removed from the list
            // of free interim pages.
            
            // For the pages we've moved, we've locked them. We've moved them in
            // the pager and have copied them to their new place on disk. The
            // copy may not have been forced yet, but that is not necessary
            // since no one is in the midst of a commit.
            
            // Now we set the pages, not load them, creating a new blank and
            // empty data page. This does not need to be forced, since the pages
            // that we are initializing are interim pages that are now
            // unreferenced, they will be cleaned up.
            
            // TODO The data to interim page boundary may be determined by
            // scanning through the data pages. If that is the case, then we
            // need to write pages out in page order, so that means a sorted set
            // or map in dirty page map.
            
            // Synapse: Why are creating the pages here when they are temporary?
            // Can't we wait until we really need them? No. You would need to
            // keep the fact that they are uninitialized around somehow, and
            // write that into the journal. Just get it over with.

            for (long gathered: setOfGathered)
            {
                pager.setPage(gathered, new DataPage(false), dirtyPages);
            }
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
                        DataPage dataPage = pager.getPage(addressPage.dereference(address), new DataPage(false));
                        
                        int length = dataPage.getSize(position, address);
                        
                        long write = writePagesBySize.bestFit(length);
                        if (write == 0L)
                        {
                            writePage = pager.newSystemPage(new DataPage(true), dirtyPages);
                            moveRecorder.getPageRecorder().getAllocationPageSet().add(write);
                        }
                        else
                        {
                            writePage = pager.getPage(write, new DataPage(true));
                        }
                        
                        writePage.allocate(address, length, dirtyPages);
                        
                        position = new MovablePosition(moveRecorder.getMoveNode(), write);
                        mapOfAddresses.put(address, position);
                    }
                    else
                    {
                        writePage  = pager.getPage(position.getValue(pager), new DataPage(true));
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
    
    public static class ByteBufferInputStream
    extends InputStream
    {
        private final ByteBuffer bytes;

        public ByteBufferInputStream(ByteBuffer bytes, boolean withCount)
        {
            if (withCount)
            {
                bytes = bytes.slice();
                bytes.limit(Integer.SIZE / Byte.SIZE + bytes.getInt());
            }
            this.bytes = bytes;
        }

        public int read() throws IOException
        {
            if (!bytes.hasRemaining())
            {
                return -1;
            }
            return bytes.get() & 0xff;
        }

        public int read(byte[] b, int off, int len) throws IOException
        {
            len = Math.min(len, bytes.remaining());
            if (len == 0)
            {
                return -1;
            }
            bytes.get(b, off, len);
            return len;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
