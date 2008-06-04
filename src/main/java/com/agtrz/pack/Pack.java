/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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
    public final static long NULL_ADDRESS = 0L;

    public final static int ERROR_FREED_FREE_ADDRESS = 300;
    
    public final static int ERROR_FREED_STATIC_ADDRESS = 301;

    public final static int ERROR_READ_FREE_ADDRESS = 302;

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
    
    public final static int ERROR_HEADER_CORRUPT = 600;

    public final static int ERROR_BLOCK_PAGE_CORRUPT = 601;
    
    public final static int ERROR_CORRUPT = 602;

    private final static long SIGNATURE = 0xAAAAAAAAAAAAAAAAL;
    
    private final static int SOFT_SHUTDOWN = 0xAAAAAAAA;

    private final static int HARD_SHUTDOWN = 0x55555555;
    
    private final static int FLAG_SIZE = 2;

    private final static int COUNT_SIZE = 4;

    private final static int POSITION_SIZE = 8;

    private final static int CHECKSUM_SIZE = 8;

    public final static int ADDRESS_SIZE = Long.SIZE / Byte.SIZE;

    private final static int FILE_HEADER_SIZE = COUNT_SIZE * 5 + ADDRESS_SIZE * 4;

    private final static int BLOCK_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;

    private final static int BLOCK_HEADER_SIZE = POSITION_SIZE + COUNT_SIZE;

    private final static short ADD_VACUUM = 1;

    private final static short VACUUM = 2;

    private final static short ADD_MOVE = 3;

    private final static short SHIFT_MOVE = 4;

    private final static short CREATE_ADDRESS_PAGE = 5;
    
    private final static short WRITE = 6;
    
    private final static short FREE = 7;

    private final static short NEXT_PAGE = 8;

    private final static short COPY = 9;

    private final static short TERMINATE = 10;

    private final static int NEXT_PAGE_SIZE = FLAG_SIZE + ADDRESS_SIZE;

    private final static int ADDRESS_PAGE_HEADER_SIZE = CHECKSUM_SIZE;

    private final static int JOURNAL_PAGE_HEADER_SIZE = CHECKSUM_SIZE + COUNT_SIZE;
    
    private final static int COUNT_MASK = 0xA0000000;
    
    final Pager pager;
    
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
        return listOfMoves.mutate(new GuardedReturnable<Mutator>()
        {
            public Mutator run(List<MoveLatch> listOfMoveLatches)
            {
                MoveNode moveNode = new MoveNode();
                DirtyPageMap dirtyPages = new DirtyPageMap(pager, 16);
                Journal journal = new Journal(pager, pageRecorder, moveNode, dirtyPages);
                return new Mutator(pager, listOfMoves, pageRecorder, journal, moveNode, dirtyPages);
            }
        });
    }

    /**
     * Soft close of the pack will wait until all mutators commit or rollback
     * and then compact the pack before closing the file.
     */
    public void close()
    {
        pager.close();
    }
    
    public File getFile()
    {
        return pager.getFile();
    }

    public void copacetic()
    {
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
 
    final static class Offsets
    {
        private final int pageSize;
        
        private final int internalJournalCount;
        
        private final int staticPageMapSize;
        
        public Offsets(int pageSize, int internalJournalCount, int staticPageMapSize)
        {
            this.pageSize = pageSize;
            this.internalJournalCount = internalJournalCount;
            this.staticPageMapSize = staticPageMapSize;
        }
        
        public int getPageSize()
        {
            return pageSize;
        }
        
        public int getInternalJournalCount()
        {
            return internalJournalCount;
        }
        private long getEndOfHeader()
        {
            return FILE_HEADER_SIZE + staticPageMapSize + internalJournalCount * POSITION_SIZE; 
        }
        
        public long getFirstAddressPageStart()
        {
            long endOfHeader = getEndOfHeader();
            long remaining = pageSize - endOfHeader % pageSize;
            if (remaining < ADDRESS_PAGE_HEADER_SIZE + ADDRESS_SIZE)
            {
                return  (endOfHeader + pageSize - 1) / pageSize * pageSize;
            }
            return endOfHeader;
        }
        
        public long getFirstAddressPage()
        {
            return getFirstAddressPageStart() / pageSize * pageSize;
        }

        public long getDataBoundary()
        {
            return (getFirstAddressPageStart() + pageSize - 1) / pageSize * pageSize;
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
                size += entry.getKey().toString().length() * 2;
            }
            return size;
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
            
            Offsets offsets = new Offsets(pageSize, internalJournalCount, getStaticPagesMapSize());
            ByteBuffer fullSize = ByteBuffer.allocateDirect((int) (offsets.getFirstAddressPage() + pageSize));
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
            header.setFirstAddressPageStart(offsets.getFirstAddressPageStart());
            header.setDataBoundary(offsets.getDataBoundary());
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
            setOfAddressPages.add(offsets.getFirstAddressPage());
            Pager pager = new Pager(file, fileChannel, disk, header, mapOfStaticPages, setOfAddressPages, header.getDataBoundary(), header.getDataBoundary());
            
            long user = pager.getInterimBoundary().getPosition();
            pager.getInterimBoundary().increment();
            
            DirtyPageMap dirtyPages = new DirtyPageMap(pager, 0);
            pager.setPage(user, new BlockPage(false), dirtyPages, false);
            BlockPage blocks = pager.getPage(user, new BlockPage(false));
            blocks.getRawPage().invalidate(0, pageSize);
            dirtyPages.flush();
            
            pager.returnUserPage(blocks);
            
            Pack pack = new Pack(pager);

            ByteBuffer statics = ByteBuffer.allocateDirect(getStaticPagesMapSize());
            
            statics.putInt(mapOfStaticPageSizes.size());
            
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
        
        public Set<Long> getTemporaryBlocks()
        {
            return Collections.emptySet();
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
                return hardOpen(file, fileChannel, header);
            }

            return softOpen(file, fileChannel, header);
       }

        private Pack softOpen(File file, FileChannel fileChannel, Header header)
        {
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
            
            Pager pager = new Pager(file, fileChannel, disk, header, mapOfStaticPages, setOfAddressPages, header.getDataBoundary(), header.getOpenBoundary());
            
            int blockPageCount = reopen.getInt();
            for (int i = 0; i < blockPageCount; i++)
            {
                long position = reopen.getLong();
                BlockPage blockPage = pager.getPage(position, new BlockPage(false));
                pager.returnUserPage(blockPage);
            }
            
            try
            {
                disk.truncate(fileChannel, header.getOpenBoundary());
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_TRUNCATE, e);
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
        
        public boolean isBlockPage(RawPage rawPage, Recovery recovery)
        {
            long position = rawPage.getPosition();
            long first = rawPage.getPager().getFirstAddressPageStart();
            if (position < first)
            {
                int pageSize = rawPage.getPager().getPageSize();
                if (position == first / pageSize * pageSize)
                {
                    return false;
                }
                else
                {
                    throw new IllegalStateException();
                }
            }
            else
            {
                ByteBuffer peek = rawPage.getByteBuffer();
                if (peek.getInt(CHECKSUM_SIZE) < 0)
                {
                    return true;
                }
            }
            return false;
        }

        private Pack addressRecover(Recovery recovery)
        {
            // Clever data structure to track data pages has count of 
            // blocks, maps page to count of blocks. If not exist, check
            // and add with one less, then decrement each time checked.
            // Uh, yeah, so I don't checksum and recover twice. Or else,
            // just checksum and recover, hey, it's just a more convoluted load.
            // Definitely keep a set of failed checksums.
            Pager pager = recovery.getPager();
            
            long position = (pager.getUserBoundary().getPosition() - 1) / pager.getPageSize() * pager.getPageSize();
            
            RawPage rawPage = new RawPage(pager, position);
            if (isBlockPage(rawPage, recovery))
            {
                throw new Danger(ERROR_CORRUPT);
            }
            
            if (new AddressPage().verifyChecksum(rawPage, recovery))
            {
                AddressPage addresses = pager.getPage(position, new AddressPage());
                addresses.verifyAddresses(recovery);
            }
            else
            {
                recovery.badAddressChecksum(position);
            }
            
            position += recovery.getPager().getPageSize();

            while (position < recovery.getFileSize())
            {
                rawPage = new RawPage(pager, position);
                BlockPage test = new BlockPage(false);
                test.verifyChecksum(rawPage, recovery);
                if (isBlockPage(rawPage, recovery))
                {
                    break;
                }
                else
                {
                    AddressPage addresses = new AddressPage();
                    if (addresses.verifyChecksum(rawPage, recovery))
                    {
                        addresses = pager.getPage(position, addresses);
                        if (addresses.verifyAddresses(recovery))
                        {
                            if (addresses.getFreeCount() != 0)
                            {
                                pager.addAddressPage(addresses);
                            }
                        }
                    }
                    else
                    {
                        recovery.badAddressChecksum(position);
                    }
                }
                position += recovery.getPager().getPageSize();
                pager.getUserBoundary().increment();
                pager.getInterimBoundary().increment();
            }

            for (;;)
            {
                pager.getInterimBoundary().increment();
                BlockPage blocks = new BlockPage(false);
                if (blocks.verifyChecksum(rawPage, recovery))
                {
                    blocks = pager.getPage(position, blocks);
                    if (blocks.verifyAddresses(recovery))
                    {
                        pager.returnUserPage(blocks);
                    }
                }
                else
                {
                    recovery.badUserChecksum(position);
                }
                position += recovery.getPager().getPageSize();
                if (position >= recovery.getFileSize())
                {
                    break;
                }
                rawPage = new RawPage(pager, position);
                if (!isBlockPage(rawPage, recovery))
                {
                    break;
                }
            }
            
            pager.close();
            
            if (recovery.copacetic())
            {
                return open(pager.getFile());
            }
            
            return null;
        }
        
        private Pack journalRecover(Recovery recovery)
        {
            throw new UnsupportedOperationException();
        }
        
        private Pack hardOpen(File file, FileChannel fileChannel, Header header)
        {
            if (header.getInternalJournalCount() < 0)
            {
                throw new Danger(ERROR_HEADER_CORRUPT);
            }
            ByteBuffer journals = ByteBuffer.allocate(header.getInternalJournalCount() * POSITION_SIZE);
            try
            {
                disk.read(fileChannel, journals, FILE_HEADER_SIZE);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_READ, e);
            }
            journals.flip();
            List<Long> listOfUserJournals = new ArrayList<Long>();
            List<Long> listOfAddressJournals = new ArrayList<Long>();
            while (journals.remaining() != 0)
            {
                long journal = journals.getLong();
                if (journal < 0)
                {
                    listOfAddressJournals.add(journal);
                }
                else if (journal > 0)
                {
                    listOfUserJournals.add(journal);
                }
            }
            long fileSize;
            try
            {
                fileSize = disk.size(fileChannel);
            }
            catch (IOException e)
            {
                throw new Danger(ERROR_IO_SIZE, e);
            }
 
            Offsets offsets = new Offsets(header.getPageSize(), header.getInternalJournalCount(), header.getStaticPageSize());
            // First see if we can get to the data section cleanly.We're not
            // going to anything but checksum.
            
            Pager pager = new Pager(file, fileChannel, disk, header, new HashMap<URI, Long>(), new TreeSet<Long>(), offsets.getDataBoundary(), offsets.getDataBoundary());
            if (listOfAddressJournals.size() == 0 && listOfUserJournals.size() == 0)
            {
                return addressRecover(new Recovery(pager, fileSize, true));
            }

            return journalRecover(new Recovery(pager, fileSize, false));
        }
    }

    final static class Pager
    {
        private final File file;

        private final FileChannel fileChannel;

        private final Disk disk;
        
        private final Header header;

        private final Map<URI, Long> mapOfStaticPages;

        private final int pageSize;

        private final int alignment;
        
        private final PositionSet setOfJournalHeaders;

        private final Map<Long, PageReference> mapOfPagesByPosition;

        private final ReferenceQueue<RawPage> queue;

        private final Boundary userBoundary;
        
        private final Boundary interimBoundary;
            
        private final MoveList listOfMoves;

        private final SortedSet<Long> setOfAddressPages;
        
        private final Set<Long> setOfReturningAddressPages;
        
        public final BySizeTable pagesBySize_;

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
        
        public Pager(File file, FileChannel fileChannel, Disk disk, Header header, Map<URI, Long> mapOfStaticPages, SortedSet<Long> setOfAddressPages, long dataBoundary, long interimBoundary)
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
            this.setOfJournalHeaders = new PositionSet(FILE_HEADER_SIZE, header.getInternalJournalCount());
            this.setOfAddressPages = setOfAddressPages;
            this.setOfReturningAddressPages = new HashSet<Long>();
        }
        
        public File getFile()
        {
            return file;
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

        // FIXME Rename.
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
            PageReference pageReference = null;
            while ((pageReference = (PageReference) queue.poll()) != null)
            {
                mapOfPagesByPosition.remove(pageReference.getPosition());
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
        public <T extends Page> T newInterimPage(T page, DirtyPageMap dirtyPages)
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
            PageReference chunkReference = (PageReference) mapOfPagesByPosition.get(boxPosition);
            if (chunkReference != null)
            {
                page = (RawPage) chunkReference.get();
            }
            return page;
        }

        @SuppressWarnings("unchecked")
        private <P extends Page> P castPage(Page page, P subtype)
        {
            return (P) page;
        }

        public <P extends Page> P getPage(long position, P page)
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
            return castPage(rawPage.getPage(), page);
        }

        public <P extends Page> P setPage(long position, P page, DirtyPageMap dirtyPages, boolean extant)
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

            return castPage(rawPage.getPage(), page);
        }
        
        private AddressPage tryGetAddressPage(long lastSelected)
        {
            synchronized (setOfAddressPages)
            {
                long position = 0L;
                if (setOfAddressPages.size() == 0 && setOfReturningAddressPages.size() == 0)
                {
                    final PageRecorder pageRecorder = new PageRecorder();
                    final MoveList listOfMoves = new MoveList(pageRecorder, getMoveList());
                    Mutator mutator = listOfMoves.mutate(new GuardedReturnable<Mutator>()
                    {
                        public Mutator run(List<MoveLatch> listOfMoveLatches)
                        {
                            MoveNode moveNode = new MoveNode();
                            DirtyPageMap dirtyPages = new DirtyPageMap(Pager.this, 16);
                            Journal journal = new Journal(Pager.this, pageRecorder, moveNode, dirtyPages);
                            return new Mutator(Pager.this, listOfMoves, pageRecorder, journal, moveNode, dirtyPages);
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
                AddressPage addressPage = getPage(position, new AddressPage());
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
                
                size += COUNT_SIZE + setOfAddressPages.size() * POSITION_SIZE;
                size += COUNT_SIZE + (setOfFreeUserPages.size() + getFreePageBySize().getSize()) * POSITION_SIZE;
                
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
                
                header.setDataBoundary(getUserBoundary().getPosition());
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
    }

    static abstract class Regional
    {
        private long position;
        
        final SortedMap<Integer, Integer> setOfRegions;
        
        public Regional(long position)
        {
            this.position = position;
            this.setOfRegions = new TreeMap<Integer, Integer>();
        }
        
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

        public abstract ByteBuffer getByteBuffer();

        public void invalidate(int offset, int length)
        {
            int start = offset;
            int end = offset + length;
            if (start < 0)
            {
                throw new IllegalStateException();
            }
            
            if (end > getByteBuffer().capacity())
            {
                throw new IllegalStateException();
            }
            
            INVALIDATE: for(;;)
            {
                Iterator<Map.Entry<Integer, Integer>> entries = setOfRegions.entrySet().iterator();
                while (entries.hasNext())
                {
                    Map.Entry<Integer, Integer> entry = entries.next();
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
            bytes.clear(); // FIXME Shouldn't be necessary.

            for(Map.Entry<Integer, Integer> entry: setOfRegions.entrySet())
            {
                bytes.limit(entry.getValue());
                bytes.position(entry.getKey());
                
                disk.write(fileChannel, bytes, getPosition() + entry.getKey());
            }

            bytes.limit(bytes.capacity());
            
            setOfRegions.clear();
        }
    }
     
    final static class Header extends Regional
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

        // FIXME Make this a checksum.
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
     * See {@link Pack.BlockPage} for more details on the separate soft
     * references.
     */
    static final class RawPage extends Regional
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
            ByteBuffer bytes = ByteBuffer.allocateDirect(pageSize);
            try
            {
                pager.getDisk().read(pager.getFileChannel(), bytes, getPosition());
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
         * file. We keep a hard reference to the raw page and the byte buffer
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
    interface Page
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
        
        public void checksum(Checksum checksum);
        
        public boolean verifyChecksum(RawPage rawPage, Recovery recovery);
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
    static final class AddressPage
    implements Page
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

        public void load(RawPage rawPage)
        {
            ByteBuffer bytes = rawPage.getByteBuffer();

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
        
        public RawPage getRawPage()
        {
            return rawPage;
        }

        public int getFreeCount()
        {
            synchronized (getRawPage())
            {
                return freeCount;
            }
        }

        public void checksum(Checksum checksum)
        {
            checksum.reset();
            ByteBuffer bytes = getRawPage().getByteBuffer();
            bytes.position(getFirstAddressOffset(getRawPage()));
            while (bytes.remaining() != 0)
            {
                checksum.update(bytes.get());
            }
            bytes.putLong(getHeaderOffset(getRawPage()), checksum.getValue());
            getRawPage().invalidate(getHeaderOffset(getRawPage()), CHECKSUM_SIZE);
        }
        
        /**
         * Adjust the starting offset for addresses in the address page
         * accounting for the header and for the file header, if this is the
         * first address page in file.
         *
         * @return The start offset for iterating through the addresses.
         */
        private int getFirstAddressOffset(RawPage rawPage)
        {
            return getHeaderOffset(rawPage) + ADDRESS_PAGE_HEADER_SIZE;
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
                bytes.clear();

                // Iterate the page buffer looking for a zeroed address that has
                // not been reserved, reserving it and returning it if found.
                
                for (int offset = getFirstAddressOffset(getRawPage()); offset < bytes.capacity(); offset += ADDRESS_SIZE)
                {
                    if (bytes.getLong(offset) == 0L)
                    {
                        dirtyPages.add(getRawPage());
                        bytes.putLong(offset, Long.MAX_VALUE);
                        getRawPage().invalidate(offset, POSITION_SIZE);
                        freeCount--;
                        return getRawPage().getPosition() + offset;
                    }
                }

                throw new IllegalStateException();
            }
        }

        public void set(long address, long position, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = rawPage.getByteBuffer();
                int offset = (int) (address - rawPage.getPosition());
                bytes.putLong(offset, position);
                getRawPage().invalidate(offset, POSITION_SIZE);
                dirtyPages.add(getRawPage());
            }
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
                return getRawPage().getByteBuffer().getLong(offset);
            }
        }

        public void free(long address, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = rawPage.getByteBuffer();
                int offset = (int) (address - rawPage.getPosition());
                long position = bytes.getLong(offset);
                if (position != 0L)
                {
                    bytes.putLong(offset, 0L);
                    
                    getRawPage().invalidate(offset, POSITION_SIZE);
                    dirtyPages.add(getRawPage());
                    
                    freeCount++;
                }
            }
        }

        public boolean verifyChecksum(RawPage rawPage, Recovery recovery)
        {
            Checksum checksum = recovery.getChecksum();
        
            ByteBuffer bytes = rawPage.getByteBuffer();
            bytes.clear();
            bytes.position(getFirstAddressOffset(rawPage));
            while (bytes.remaining() != 0)
            {
                checksum.update(bytes.get());
            }
            
            long actual = bytes.getLong(getHeaderOffset(rawPage));
            long expected = checksum.getValue();
            
            if (actual != expected)
            {
                recovery.badAddressChecksum(rawPage.getPosition());
                return false;
            }
            
            return true;
        }

        public boolean verifyAddresses(Recovery recovery)
        {
            boolean copacetic = true;
        
            ByteBuffer bytes = getRawPage().getByteBuffer();
        
            bytes.clear();
            bytes.position(getFirstAddressOffset(rawPage));
            
            Pager pager = getRawPage().getPager();
            while (bytes.remaining() >= Long.SIZE / Byte.SIZE)
            {
                long address = getRawPage().getPosition() + bytes.position();
                long position = bytes.getLong();
                if (position == 0)
                {
                }
                else if (position < pager.getPageSize())
                {
                    copacetic = false;
                }
                else if (position > recovery.getFileSize())
                {
                    copacetic = false;
                }
                else
                {
                    RawPage rawPage = new RawPage(getRawPage().getPager(), position);
                    BlockPage blocks = new BlockPage(false);
                    if (!blocks.verifyChecksum(rawPage, recovery)
                        || !pager.getPage(position, blocks).contains(address))
                    {
                        copacetic = false;
                        recovery.badAddress(address, position);
                    }
                }
            }
            
            return copacetic;
        }
    }
    
    static class RelocatablePage
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
        
        public void checksum(Checksum checksum)
        {
            throw new UnsupportedOperationException();
        }
        
        public boolean verifyChecksum(RawPage rawPage, Recovery recovery)
        {
            throw new UnsupportedOperationException();
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
            bytes.clear();
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
     */
    static final class BlockPage
    extends RelocatablePage
    {
        private boolean interim;

        /**
         * True if the page is in the midst of a vacuum and should not be written to.
         */
        private long mirror;

        private int count;

        private int remaining;

        public BlockPage(boolean interim)
        {
            this.interim = interim;
        }
        
        private int getDiskCount()
        {
            if (interim)
            {
                return count;
            }
            return count | COUNT_MASK;
        }

        public void create(RawPage rawPage, DirtyPageMap dirtyPages)
        {
            super.create(rawPage, dirtyPages);
            
            this.count = 0;
            this.remaining = rawPage.getPager().getPageSize() - BLOCK_PAGE_HEADER_SIZE;
            
            ByteBuffer bytes = rawPage.getByteBuffer();

            bytes.clear();

            rawPage.invalidate(0, BLOCK_PAGE_HEADER_SIZE);
            bytes.putLong(0L);
            bytes.putInt(getDiskCount());
            
            dirtyPages.add(rawPage);
        }

        private int getConsumed()
        {
            int consumed = BLOCK_PAGE_HEADER_SIZE;
            ByteBuffer bytes = getBlockRange();
            for (int i = 0; i < count; i++)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    consumed += size;
                }
                advance(bytes, size);
            }
            return consumed;
        }

        public void load(RawPage rawPage)
        {    
            super.load(rawPage);

            ByteBuffer bytes = rawPage.getByteBuffer();

            bytes.clear();
            bytes.getLong();

            count = bytes.getInt();

            if ((count & COUNT_MASK) != 0)
            {
                if (interim)
                {
                    throw new IllegalStateException();
                }
                count = count & ~COUNT_MASK;
            }
            else if (!interim)
            {
                throw new IllegalStateException();
            }

            this.remaining = getRawPage().getPager().getPageSize() - getConsumed();
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

        @Override
        public void checksum(Checksum checksum)
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            bytes.putLong(0, getChecksum(checksum));
            getRawPage().invalidate(0, CHECKSUM_SIZE);
        }

        private int getBlockSize(ByteBuffer bytes)
        {
            int size = bytes.getInt(bytes.position());
            if (Math.abs(size) > bytes.remaining())
            {
                throw new IllegalStateException();
            }
            return size;
        }

        private long getAddress(ByteBuffer bytes)
        {
            return bytes.getLong(bytes.position() + COUNT_SIZE);
        }
        
        private void advance(ByteBuffer bytes, int blockSize)
        {
            bytes.position(bytes.position() + Math.abs(blockSize));
        }

        /**
         * Return the byte buffer associated with this data page with the
         * position and limit set to the range of bytes that contain blocks.
         *
         * @return The byte buffer limited to the block range.
         */
        private ByteBuffer getBlockRange(ByteBuffer bytes)
        {
            bytes.position(BLOCK_PAGE_HEADER_SIZE);
            return bytes;
        }

        private ByteBuffer getBlockRange()
        {
            return getBlockRange(getRawPage().getByteBuffer());
        }

        private boolean unmoved()
        {
            return getRawPage().getPage() == this;
        }

        /**
         * Advance to the block associated with the address in this page. If
         * found the position of the byte buffer will be at the start of the
         * full block including the block header. If not found the block is
         * after the last valid block.
         * 
         * @param bytes
         *            The byte buffer of this block page.
         * @param address
         *            The address to seek.
         * @return True if the address is found, false if not found.
         */
        private boolean seek(ByteBuffer bytes, long address)
        {
            bytes = getBlockRange(bytes);
            int block = 0;
            while (block < count)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    block++;
                }
                if (getAddress(bytes) == address)
                {
                    return true;
                }
                advance(bytes, size);
            }
            return false;
        }
        
        public boolean contains(long address)
        {
            waitOnMirror(-1L);
            return unmoved() && seek(getRawPage().getByteBuffer(), address);
        }
        
        public int getSize(long address)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    return getBlockSize(bytes);
                }
            }
            throw new IllegalArgumentException();
        }

        public List<Long> getAddresses()
        {
            List<Long> listOfAddresses = new ArrayList<Long>(getCount());
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getBlockRange();
                int block = 0;
                while (block < getCount())
                {
                    int size = getBlockSize(bytes);
                    if (size > 0)
                    {
                        block++;
                        listOfAddresses.add(getAddress(bytes));
                    }
                    advance(bytes, size);
                }
            }
            return listOfAddresses;
        }

        /**
         * Allocate a block that is referenced by the specified address.
         * 
         * @param address
         *            The address that will reference the newly allocated block.
         * @param blockSize
         *            The full block size including the block header.
         * @param dirtyPages
         *            A dirty page map to record the block page if it changes.
         * @return True if the allocation is successful.
         */
        public void allocate(long address, int blockSize, DirtyPageMap dirtyPages)
        {
            if (blockSize < BLOCK_HEADER_SIZE)
            {
                throw new IllegalArgumentException();
            }

            synchronized (getRawPage())
            {
                if (mirror != 0)
                {
                    throw new IllegalStateException();
                }
        
                ByteBuffer bytes = getBlockRange();
                boolean found = false;
                int block = 0;
                // FIXME Not finding anymore. That's taken care of in commit.
                while (block != count && !found)
                {
                    int size = getBlockSize(bytes);
                    if (size > 0)
                    {
                        block++;
                        if(getAddress(bytes) == address)
                        {
                            found = true;
                        }
                    }
                    bytes.position(bytes.position() + Math.abs(size));
                }
        
                if (!found)
                {
                    getRawPage().invalidate(bytes.position(), blockSize);
        
                    bytes.putInt(blockSize);
                    bytes.putLong(address);
        
                    count++;
                    remaining -= blockSize;
        
                    bytes.clear();
                    bytes.putInt(CHECKSUM_SIZE, interim ? count : count | COUNT_MASK);
                    getRawPage().invalidate(CHECKSUM_SIZE, COUNT_SIZE);
        
                    dirtyPages.add(getRawPage());
                }
            }
        }
        
        private void waitOnMirror(long caller)
        {
            if (caller != mirror)
            {
                while (mirror != 0)
                {
                    try
                    {
                        getRawPage().wait();
                    }
                    catch (InterruptedException e)
                    {
                    }
                }
            }
        }
        
        public void write(long address, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    int blockSize = getBlockSize(bytes);
                    bytes.limit(bytes.position() + blockSize);
                    bytes.position(bytes.position() + BLOCK_HEADER_SIZE);
                    

                    Pager pager = getRawPage().getPager();
                    AddressPage addresses = pager.getPage(address, new AddressPage());
                    long lastPosition = 0L;
                    for (;;)
                    {
                        long actual = addresses.dereference(address);
                        if (actual == 0L || actual == Long.MAX_VALUE)
                        {
                            throw new Danger(ERROR_READ_FREE_ADDRESS);
                        }
                        
                        if (actual != lastPosition)
                        {
                            BlockPage blocks = pager.getPage(actual, new BlockPage(false));
                            synchronized (blocks.getRawPage())
                            {
                                if (blocks.write(address, bytes, dirtyPages))
                                {
                                    break;
                                }
                            }
                            lastPosition = actual;
                        }
                        else
                        {
                            throw new IllegalStateException();
                        }
                    }

                    bytes.limit(bytes.capacity());
                }
            }
        }

        public boolean write(long address, ByteBuffer data, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                waitOnMirror(-1L); // Actually never invoked.
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    int offset = bytes.position();
                    int size = bytes.getInt();
                    if (bytes.getLong() != address)
                    {
                        throw new Danger(ERROR_BLOCK_PAGE_CORRUPT);
                    }
                    bytes.limit(offset + size);
                    getRawPage().invalidate(bytes.position(), bytes.remaining());
                    bytes.put(data);
                    bytes.limit(bytes.capacity());
                    dirtyPages.add(getRawPage());
                    return true;
                }
                return false;
            }
        }

        // FIXME Why is this boolean. We should never ask for an address that 
        // does not exist.
        public boolean read(long address, ByteBuffer dst)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    int offset = bytes.position();
                    int size = bytes.getInt();
                    if (bytes.getLong() != address)
                    {
                        throw new IllegalStateException();
                    }
                    bytes.limit(offset + size);
                    dst.put(bytes);
                    bytes.limit(bytes.capacity());
                    return true;
                }
            }
            return false;
        }
        
        // Note that this must be called in a synchronized block.
        public long getChecksum(Checksum checksum)
        {
            checksum.reset();

            ByteBuffer bytes = getRawPage().getByteBuffer();
            bytes.clear();
            bytes.position(CHECKSUM_SIZE);
            
            for (int i = 0; i < COUNT_SIZE; i++)
            {
                checksum.update(bytes.get());
            }
            
            int block = 0;
            while (block < count)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    for (int i = 0; i < size; i++)
                    {
                        checksum.update(bytes.get());
                    }
                    block++;
                }
                else
                {
                    bytes.position(bytes.position() + -size);
                }
            }
            
            return checksum.getValue();
        }

        /**
         * Mirror the page excluding freed blocks.
         * <p>
         * For a time, this method would copy starting after the first free
         * block with the intention of making vacuum more efficient. However,
         * copying over the entire page makes recovery more certain. Generating
         * a checksum for the expected page.
         * 
         * @param pager
         * @param dirtyPages
         * @param force
         * @return
         */
        public Mirror mirror(Pager pager, BlockPage mirrored, boolean force, DirtyPageMap dirtyPages)
        {
            int offset = force ? 0 : -1;
            
            synchronized (getRawPage())
            {
                if (mirror != 0)
                {
                    throw new IllegalStateException();
                }

                ByteBuffer bytes = getBlockRange();
                int block = 0;
                while (block != count)
                {
                    int size = getBlockSize(bytes);
                    if (size < 0)
                    {
                        offset = block;
                        advance(bytes, size);
                    }
                    else
                    {
                        block++;
                        if (size > remaining)
                        {
                            throw new IllegalStateException();
                        }
                        if (offset == -1)
                        {
                            advance(bytes, size);
                        }
                        else
                        {
                            if (mirrored == null)
                            {
                                mirrored = pager.newInterimPage(new BlockPage(true), dirtyPages);
                            }

                            int blockSize = bytes.getInt();
                            long address = bytes.getLong();
                            
                            mirrored.allocate(address, blockSize, dirtyPages);

                            int userSize = blockSize - BLOCK_HEADER_SIZE;

                            bytes.limit(bytes.position() + userSize);
                            mirrored.write(address, bytes, dirtyPages);
                            bytes.limit(bytes.capacity());
                        }
                    } 
                }
                
                if (mirrored != null)
                {
                    mirror = mirrored.getRawPage().getPosition();
                    long checksum = getChecksum(dirtyPages.getChecksum());
                    return new Mirror(mirrored, offset, checksum);
                }
            }
            
            return null;
        }
        
        public boolean free(long address, DirtyPageMap dirtyPages)
        {
            return free(address, dirtyPages, -1L);
        }

        public boolean free(long address, DirtyPageMap dirtyPages, long caller)
        {
            synchronized (getRawPage())
            {
                waitOnMirror(caller);
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    int offset = bytes.position();

                    int size = bytes.getInt();
                    if (size > 0)
                    {
                        size = -size;
                    }

                    getRawPage().invalidate(offset, COUNT_SIZE);
                    bytes.putInt(offset, size);
                    
                    count--;
                    getRawPage().invalidate(CHECKSUM_SIZE, COUNT_SIZE);
                    bytes.putInt(CHECKSUM_SIZE, interim ? count : count | COUNT_MASK);

                    dirtyPages.add(getRawPage());
                    return true;
                }
            }
            return false;
        }

        public void compact(BlockPage user, DirtyPageMap dirtyPages, int offset, long checksum)
        {
            if (offset > user.count)
            {
                throw new IllegalStateException();
            }
            ByteBuffer bytes = user.getBlockRange();
            int block = 0;
            while (block < offset)
            {
                int blockSize = getBlockSize(bytes);
                if (blockSize < 0)
                {
                    throw new IllegalStateException();
                }
                advance(bytes, blockSize);
                block++;
            }
            if (user.count - block != count)
            {
                throw new IllegalStateException();
            }
            user.count = block;
            for (long address : getAddresses())
            {
                commit(address, user, dirtyPages);
            }
            if (checksum != user.getChecksum(dirtyPages.getChecksum()))
            {
                throw new IllegalStateException();
            }
        }
        
        public void commit(long address, ByteBuffer block, DirtyPageMap dirtyPages, long caller)
        {
            synchronized (getRawPage())
            {
                RawPage rawPage = getRawPage();
                Pager pager = rawPage.getPager();
                AddressPage addresses = pager.getPage(address, new AddressPage());
                long position = addresses.dereference(address);
                if (position != getRawPage().getPosition())
                {
                    if (position == 0L)
                    {
                        throw new IllegalStateException();
                    }
                    if (position != Long.MAX_VALUE)
                    {
                        BlockPage blocks = pager.getPage(position, new BlockPage(false));
                        blocks.free(address, dirtyPages, caller);
                    }
                    addresses.set(address, getRawPage().getPosition(), dirtyPages);
                }
                
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    int size = bytes.getInt();
                    
                    if (size != block.remaining() + BLOCK_HEADER_SIZE)
                    {
                        throw new IllegalStateException();
                    }
                    
                    if (bytes.getLong() != address)
                    {
                        throw new IllegalStateException();
                    }
                    
                    getRawPage().invalidate(bytes.position(), block.remaining());
                    bytes.put(block);
                }
                else
                {
                    if (block.remaining() + BLOCK_HEADER_SIZE > bytes.remaining())
                    {
                        throw new IllegalStateException();
                    }
                    
                    getRawPage().invalidate(bytes.position(), block.remaining() + BLOCK_HEADER_SIZE);
                    
                    remaining -= block.remaining() + BLOCK_HEADER_SIZE;
                    
                    bytes.putInt(block.remaining() + BLOCK_HEADER_SIZE);
                    bytes.putLong(address);
                    bytes.put(block);
                    
                    count++;
                    
                    getRawPage().invalidate(CHECKSUM_SIZE, COUNT_SIZE);
                    bytes.putInt(CHECKSUM_SIZE, interim ? count : count | COUNT_MASK);
                }

                dirtyPages.add(getRawPage());
            }
        }

        public void commit(long address, BlockPage user, DirtyPageMap dirtyPages)
        {
            // FIXME Locking a lot. Going to deadlock?
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getRawPage().getByteBuffer();
                if (seek(bytes, address))
                {
                    int offset = bytes.position();
                    
                    int blockSize = bytes.getInt();

                    bytes.position(offset + BLOCK_HEADER_SIZE);
                    bytes.limit(offset + blockSize);

                    user.commit(address, bytes.slice(), dirtyPages, getRawPage().getPosition());

                    bytes.limit(bytes.capacity());
                }
            }
        }

        public boolean verifyChecksum(RawPage rawPage, Recovery recovery)
        {
            Checksum checksum = recovery.getChecksum();
            checksum.reset();
            
            ByteBuffer bytes = rawPage.getByteBuffer();
            bytes.position(CHECKSUM_SIZE);
            
            for (int i = 0; i < Integer.SIZE / Byte.SIZE; i++)
            {
                checksum.update(bytes.get());
            }
            
            int count = bytes.getInt(CHECKSUM_SIZE);
            if ((count & COUNT_MASK) != 0)
            {
                count = (count & ~COUNT_MASK);
            }
            
            int block = 0;
            while (block < count)
            {
                int size = bytes.getInt(bytes.position());
                
                if (Math.abs(size) > bytes.remaining())
                {
                    recovery.corruptDataPage(rawPage.getPosition());
                    return false;
                }
                
                if (size > 0)
                {
                    block++;
                    for (int i = 0; i < size; i++)
                    {
                        checksum.update(bytes.get());
                    }
                }
                else
                {
                    advance(bytes, size);
                }
            }
            
            long expected = checksum.getValue();
            long actual = bytes.getLong(0);
            
            if (expected != actual)
            {
                recovery.badUserChecksum(rawPage.getPosition());
                return false;
            }
            
            return true;
        }
        
        public boolean verifyAddresses(Recovery recovery)
        {
            boolean copacetic = true;
            Pager pager = getRawPage().getPager();
            ByteBuffer bytes = getBlockRange();
            int block = 0;
            while (block < count)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    block++;
                    long address = getAddress(bytes);
                    if (address < pager.getFirstAddressPageStart() + ADDRESS_PAGE_HEADER_SIZE
                        || address >= pager.getUserBoundary().getPosition())
                    {
                        recovery.badUserAddress(getRawPage().getPosition(), address);
                        copacetic = false;
                    }
                    AddressPage addresses = pager.getPage(address, new AddressPage());
                    if (getRawPage().getPosition() != addresses.dereference(address))
                    {
                        recovery.badUserAddress(getRawPage().getPosition(), address);
                        copacetic = false;
                    }
                }
                else
                {
                    advance(bytes, size);
                }
            }
            return copacetic;
        }
    }
    
    final static class Mirror
    {
        private final BlockPage mirrored;

        private final int offset;
        
        private final long checksum;
        
        public Mirror(BlockPage mirrored, int offset, long checksum)
        {
            this.mirrored = mirrored;
            this.offset = offset;
            this.checksum = checksum;
        }
        
        public BlockPage getMirrored()
        {
            return mirrored;
        }
        
        public int getOffset()
        {
            return offset;
        }
        
        public long getChecksum()
        {
            return checksum;
        }
    }

    final static class JournalPage
    extends RelocatablePage
    {
        private int offset;

        public void create(RawPage position, DirtyPageMap dirtyPages)
        {
            super.create(position, dirtyPages);

            ByteBuffer bytes = getRawPage().getByteBuffer();
            
            bytes.clear();
            bytes.putLong(0);
            bytes.putInt(0);

            getRawPage().setPage(this);
            
            dirtyPages.add(getRawPage());
            
            this.offset = JOURNAL_PAGE_HEADER_SIZE;
        }

        public void load(RawPage position)
        {
            super.load(position);
            
            this.offset = JOURNAL_PAGE_HEADER_SIZE;
        }
        
        /**
         * Checksum the entire journal page. In order to checksum only journal
         * pages I'll have to keep track of where the journal ends.
         * 
         * @param checksum The checksum algorithm.
         */
        @Override
        public void checksum(Checksum checksum)
        {
            checksum.reset();
            ByteBuffer bytes = getRawPage().getByteBuffer();
            bytes.position(CHECKSUM_SIZE);
            while (bytes.position() != offset)
            {
                checksum.update(bytes.get());
            }
            bytes.putLong(0, checksum.getValue());
            getRawPage().invalidate(0, CHECKSUM_SIZE);
        }
        
        private ByteBuffer getByteBuffer()
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            
            bytes.clear();
            bytes.position(offset);

            return bytes;
        }

        public boolean write(Operation operation, int overhead, DirtyPageMap dirtyPages)
        {
            synchronized (getRawPage())
            {
                ByteBuffer bytes = getByteBuffer();

                if (operation.length() + overhead < bytes.remaining())
                {
                    getRawPage().invalidate(offset, operation.length());
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
                case ADD_VACUUM:
                    return new AddVacuum();
                case VACUUM:
                    return new Vacuum();
                case ADD_MOVE: 
                    return new AddMove();
                case SHIFT_MOVE:
                    return new ShiftMove();
                case CREATE_ADDRESS_PAGE:
                    return new CreateAddressPage();
                case WRITE:
                    return new Write();
                case FREE:
                    return new Free();
                case NEXT_PAGE:
                    return new NextOperation();
                case COPY:
                    return new Copy();
                case TERMINATE:
                    return new Terminate();
            }
            throw new IllegalStateException("Invalid type: " + type);
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

    public final static class DirtyPageMap
    {
        private final Pager pager;
        
        private final Checksum checksum;

        private final Map<Long, RawPage> mapOfPages;

        private final Map<Long, ByteBuffer> mapOfByteBuffers;

        private final int capacity;
        
        public DirtyPageMap(Pager pager, int capacity)
        {
            this.pager = pager;
            this.checksum = new Adler32();
            this.mapOfPages = new HashMap<Long, RawPage>();
            this.mapOfByteBuffers = new HashMap<Long, ByteBuffer>();
            this.capacity = capacity;
        }
        
        public Checksum getChecksum()
        {
            return checksum;
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
                    rawPage.getPage().checksum(getChecksum());
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

    static final class PageReference
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

    // FIXME Probably needs to just represent a single pointer.
    final static class Pointer
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

        public long getPosition()
        {
            return position;
        }

        public ByteBuffer getByteBuffer()
        {
            return slice;
        }

        public Object getMutex()
        {
            return mutex;
        }
    }

    final static class PositionSet
    {
        private final boolean[] reserved;

        private final long position;

        public PositionSet(long position, int count)
        {
            this.position = position;
            this.reserved = new boolean[count];
        }

        public int getCapacity()
        {
            return reserved.length;
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
    }
    
    final static class FreeSet
    implements Iterable<Long>
    {
        private final SortedSet<Long> setOfPositions;
        
        private final SortedSet<Long> setToIgnore;
        
        public FreeSet()
        {
            this.setOfPositions = new TreeSet<Long>();
            this.setToIgnore = new TreeSet<Long>();
        }
        
        public synchronized int size()
        {
            return setOfPositions.size();
        }
        
        public Iterator<Long> iterator()
        {
            return setOfPositions.iterator();
        }
        
        /**
         * Remove the interim page from the set of free interim pages if the
         * page is in the set of free interim pages. Returns true if the page
         * was in the set of free interim pages.
         *
         * @param position The position of the interim free page.
         */
        public synchronized boolean reserve(long position)
        {
            if (setOfPositions.remove(position))
            {
                return true;
            }
            setToIgnore.add(position);
            return false;
        }
        
        public synchronized void release(Set<Long> setToRelease)
        {
            setToIgnore.removeAll(setToRelease);
        }
        
        public synchronized long allocate()
        {
            if (setOfPositions.size() != 0)
            {
                long position = setOfPositions.first();
                setOfPositions.remove(position);
                return position;
            }
            return 0L;
        }
        
        public synchronized void free(Set<Long> setOfPositions)
        {
            for (long position : setOfPositions)
            {
                free(position);
            }
        }

        public synchronized void free(long position)
        {
            if (!setToIgnore.contains(position))
            {
                setOfPositions.add(position);
            }
        }
    }

    final static class BySizeTable implements Iterable<Long>
    {
        private final int alignment;

        private final List<LinkedList<Long>> listOfListsOfSizes;
        
        private final SortedMap<Long, Integer> setToIgnore;

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
            this.setToIgnore = new TreeMap<Long, Integer>();
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
        
        public Iterator<Long> iterator()
        {
            return new BySizeTableIterator(listOfListsOfSizes);
        }

        public synchronized void add(BlockPage blocks)
        {
            add(blocks.getRawPage().getPosition(), blocks.getRemaining());
        }
        
        public synchronized void add(long position, int remaning)
        {
            if (!setToIgnore.containsKey(position))
            {
                // Maybe don't round down if exact.
                int aligned = ((remaning | alignment - 1) + 1) - alignment;
                if (aligned != 0)
                {
                    listOfListsOfSizes.get(aligned / alignment).addFirst(position);
                }
            }
        }
        
        private boolean remove(BlockPage blocks)
        {
            // Maybe don't round down if exact.
            int aligned = ((blocks.getRemaining() | alignment - 1) + 1) - alignment;
            if (aligned != 0)
            {
                LinkedList<Long> listOfPositions = listOfListsOfSizes.get(aligned / alignment);
                return listOfPositions.remove(blocks.getRawPage().getPosition());
            }
            return false;
        }

        public synchronized boolean reserve(BlockPage blocks)
        {
            // FIXME Reserve should now add to the set to ignore always.
            if (remove(blocks))
            {
                return true;
            }
            long position = blocks.getRawPage().getPosition();
            Integer count = setToIgnore.get(position);
            setToIgnore.put(position, count == null ? 1 : count + 1);
            return false;
        }
        
        public synchronized void release(long position)
        {
            Integer count = setToIgnore.get(position);
            if (count != null)
            {
                setToIgnore.put(position, count - 1);
            }
        }

        public void release(Set<Long> setToRelease)
        {
            for (long position : setToRelease)
            {
                release(position);
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

        public synchronized void join(BySizeTable pagesBySize, Set<Long> setOfDataPages, Map<Long, Movable> mapOfPages, MoveNode moveNode)
        {
            // FIXME We can compact even further by joining vacuumed pages.
            for (int i = 0; i < pagesBySize.listOfListsOfSizes.size(); i++)
            {
                List<Long> listOfSizes = pagesBySize.listOfListsOfSizes.get(i);
                for (long size: listOfSizes)
                {
                    int alignedRemaining = i * alignment;
                    int pageSize = listOfListsOfSizes.size() * alignment;
                    int needed = pageSize - BLOCK_PAGE_HEADER_SIZE - alignedRemaining;
                    long found = bestFit(needed);
                    if (found != 0L)
                    {
                        setOfDataPages.add(found);
                        mapOfPages.put(size, new Movable(moveNode, found, 0));
                    }
                }
            }
        }
    }
    
    final static class BySizeTableIterator implements Iterator<Long>
    {
        private Iterator<LinkedList<Long>> listsOfSizes;
        
        private Iterator<Long> sizes;
        
        public BySizeTableIterator(List<LinkedList<Long>> listOfListsOfSizes)
        {
            Iterator<LinkedList<Long>> listsOfSizes = listOfListsOfSizes.iterator();
            Iterator<Long> sizes = listsOfSizes.next().iterator();
            while (!sizes.hasNext() && listsOfSizes.hasNext())
            {
                sizes = listsOfSizes.next().iterator();
            }
            this.listsOfSizes = listsOfSizes;
            this.sizes = sizes;
        }

        public boolean hasNext()
        {
            return sizes.hasNext() || listsOfSizes.hasNext();
        }
        
        public Long next()
        {
            while (!sizes.hasNext())
            {
                if (!listsOfSizes.hasNext())
                {
                    throw new ArrayIndexOutOfBoundsException();
                }
                sizes = listsOfSizes.next().iterator();
            }
            long size = sizes.next();
            while (!sizes.hasNext() && listsOfSizes.hasNext())
            {
                sizes = listsOfSizes.next().iterator();
            }
            return size;
        }
        
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    // FIXME Checksum of vacuum indicates a healthy page since no writes
    // would take place until after the vacuum commits.
    final static class Recovery
    {
        private final Pager pager;
        
        private final Checksum checksum;

        private int blockCount;

        private final Set<Long> setOfBadAddressChecksums;

        private final Map<Long, Map<Long, Long>> mapOfBadAddressPages;
        
        private final Set<Long> setOfCorruptDataPages;
        
        private final Set<Long> setOfBadUserChecksums;

        private final Map<Long, List<Long>> mapOfBadUserAddresses;
        
        private boolean addressRecovery;
        
        private long fileSize;
        
        public Recovery(Pager pager, long fileSize, boolean addressRecovery)
        {
            this.pager = pager;
            this.checksum = new Adler32();
            this.mapOfBadAddressPages = new HashMap<Long, Map<Long, Long>>();
            this.setOfBadAddressChecksums = new HashSet<Long>();
            this.setOfBadUserChecksums = new HashSet<Long>();
            this.setOfCorruptDataPages = new HashSet<Long>();
            this.mapOfBadUserAddresses = new HashMap<Long, List<Long>>();
            this.fileSize = fileSize;
            this.addressRecovery = addressRecovery;
        }

        public boolean copacetic()
        {
            return mapOfBadAddressPages.size() == 0
                && setOfBadAddressChecksums.size() == 0
                && setOfBadUserChecksums.size() == 0
                && setOfCorruptDataPages.size() == 0
                && mapOfBadUserAddresses.size() == 0;
        }

        public Pager getPager()
        {
            return pager;
        }
        
        public long getFileSize()
        {
            return fileSize;
        }
        
        public boolean isAddressRecovery()
        {
            return addressRecovery;
        }

        public Checksum getChecksum()
        {
            return checksum;
        }

        public int getBlockCount()
        {
            return blockCount;
        }
        
        public void incBlockCount()
        {
            blockCount++;
        }
        
        public void badAddressChecksum(long position)
        {
            setOfBadAddressChecksums.add(position);
        }
        
        public void badAddress(long address, long position)
        {
            long page = address / pager.getPageSize() * pager.getPageSize(); 
            Map<Long, Long> mapOfBadAddresses = mapOfBadAddressPages.get(page);
            if (mapOfBadAddresses == null)
            {
                mapOfBadAddresses = new HashMap<Long, Long>();
                mapOfBadAddressPages.put(page, mapOfBadAddresses);
            }
            mapOfBadAddresses.put(address, position);
        }

        public void corruptDataPage(long position)
        {
            setOfCorruptDataPages.add(position);
        }

        public void badUserChecksum(long position)
        {
            setOfBadUserChecksums.add(position);
        }
        
        public void badUserAddress(long position, long address)
        {
            List<Long> listOfAddresses = mapOfBadUserAddresses.get(position);
            if (listOfAddresses == null)
            {
                listOfAddresses = new ArrayList<Long>();
                mapOfBadUserAddresses.put(position, listOfAddresses);
            }
            listOfAddresses.add(address);
        }
    }

    final static class MoveList
    {
        private final MoveRecorder recorder;

        private final ReadWriteLock readWriteLock;
        
        private final List<MoveLatch> listOfMoveLatches;
        
        private MoveLatch headOfMoves;
        
        private boolean wasLocked;
        
        private boolean skipping;
        
        public MoveList()
        {
            this.recorder = new NullMoveRecorder();
            this.headOfMoves = new MoveLatch(false);
            this.readWriteLock = new ReentrantReadWriteLock();
            this.listOfMoveLatches = new ArrayList<MoveLatch>();
        }

        public MoveList(MoveRecorder recorder, MoveList listOfMoves)
        {
            this.recorder = recorder;
            this.headOfMoves = listOfMoves.headOfMoves;
            this.readWriteLock = listOfMoves.readWriteLock;
            this.listOfMoveLatches = new ArrayList<MoveLatch>(listOfMoves.listOfMoveLatches);
        }
        
        public void add(MoveLatch next)
        {
            readWriteLock.writeLock().lock();
            try
            {
                MoveLatch iterator = headOfMoves;
                while (iterator.getNext() != null)
                {
                    iterator = iterator.getNext();
                    if (iterator.isUser())
                    {
                        if (iterator.isTerminal())
                        {
                            listOfMoveLatches.clear();
                        }
                        else
                        {
                            listOfMoveLatches.add(iterator);
                        }
                    }
                }
                iterator.extend(next);
                
                headOfMoves = iterator;
            }
            finally
            {
                readWriteLock.writeLock().unlock();
            }
        }
        
        public void skip(MoveLatch skip)
        {
            wasLocked = false;
            skipping = false;
            readWriteLock.readLock().lock();
            try
            {
                for (;;)
                {
                    if (headOfMoves.next == null)
                    {
                        break;
                    }
                    else
                    {
                        advance(skip);
                    }
                }
            }
            finally
            {
                readWriteLock.readLock().unlock();
            }
        }

        public <T> T mutate(final GuardedReturnable<T> guarded)
        {
            wasLocked = false;
            skipping = false;
            readWriteLock.readLock().lock();
            try
            {
                for (;;)
                {
                    if (headOfMoves.next == null)
                    {
                        return guarded.run(listOfMoveLatches);
                    }
                    else
                    {
                        advance(null);
                    }
                }
            }
            finally
            {
                readWriteLock.readLock().unlock();
            }
        }
        
        public void mutate(Guarded guarded)
        {
            wasLocked = false;
            skipping = false;
            readWriteLock.readLock().lock();
            try
            {
                for (;;)
                {
                    if (headOfMoves.next == null)
                    {
                        guarded.run(listOfMoveLatches);
                        break;
                    }
                    else
                    {
                        advance(null);
                    }
                }
            }
            finally
            {
                readWriteLock.readLock().unlock();
            }
        }

        private void advance(MoveLatch skip)
        {
            headOfMoves = headOfMoves.next;
            if (headOfMoves.isTerminal())
            {
                if (headOfMoves.isUser())
                {
                    if (wasLocked)
                    {
                        throw new IllegalStateException();
                    }
                    listOfMoveLatches.clear();
                }
                skipping = skip == headOfMoves;
            }
            else
            {
                if (recorder.involves(headOfMoves.getMove().getFrom()))
                {
                    if (skipping)
                    {
                        if (headOfMoves.isUser() && headOfMoves.isLocked())
                        {
                            wasLocked = true;
                        }
                    }
                    else if (headOfMoves.enter())
                    {
                        if (headOfMoves.isUser())
                        {
                            wasLocked = true;
                        }
                    }
                    recorder.record(headOfMoves.getMove(), false);
                }
                else
                {
                    if (headOfMoves.isUser())
                    {
                        if (headOfMoves.isLocked())
                        {
                            wasLocked = true;
                        }
                        listOfMoveLatches.add(headOfMoves);
                    }
                }
            }
        }
    }
    
    static class Guarded
    {
        public void run(List<MoveLatch> listOfMoveLatches)
        {
        }
        
        public void run(BlockPage blocks)
        {
        }
    }
    
    static class GuardedReturnable<T>
    {
        public T run(List<MoveLatch> listOfMoveLatches)
        {
            return null;
        }
        
        public T run(BlockPage blocks)
        {
            return null;
        }
    }
   
    static final class Move
    {
        private final long from;
        
        private final long to;
        
        public Move(long from, long to)
        {
            if (from == to || from == 0 || to == 0)
            {
                throw new IllegalArgumentException();
            }

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

    static final class MoveLatch
    {
        private final boolean terminal;
        
        private final boolean user;

        private final Move move;

        private boolean locked;
        
        private MoveLatch next;
        
        public MoveLatch(boolean user)
        {
            this.terminal = true;
            this.user = user;
            this.locked = false;
            this.move = null;
        }

        public MoveLatch(Move move, boolean user)
        {
            this.move = move;
            this.locked = true;
            this.user = user;
            this.terminal = false;
        }

        public boolean isTerminal()
        {
            return terminal;
        }
        
        public boolean isUser()
        {
            return user;
        }

        public synchronized boolean isLocked()
        {
            return locked;
        }
        
        public synchronized void unlatch()
        {
            locked = false;
            notifyAll();
        }
        
        public synchronized boolean enter()
        {
            boolean wasLocked = locked;
            while (locked)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException e)
                {
                }
            }
            return wasLocked;
        }

        public Move getMove()
        {
            return move;
        }
        
        public void extend(MoveLatch next)
        {
            assert this.next == null;
        
            this.next = next;
        }

        public MoveLatch getNext()
        {
            return next;
        }

        public MoveLatch getLast()
        {
            MoveLatch iterator = this;
            while (iterator.next != null)
            {
                iterator = iterator.next;
            }
            return iterator;
        }
    }
    
    static final class MoveNode
    {
        private final Move move;
        
        private MoveNode next;
        
        public MoveNode()
        {
            this.move = null;
        }
 
        public MoveNode(Move move)
        {
            this.move = move;
        }
        
        public Move getMove()
        {
            return move;
        }
        
        public MoveNode extend(Move move)
        {
            assert next == null;
            
            return next = new MoveNode(move);
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
    }

    static class Movable
    {
        private final MoveNode moveNode;
        
        private final long position;
        
        private final int skip;
        
        public Movable(MoveNode moveNode, long position, int skip)
        {
            this.moveNode = moveNode;
            this.position = position;
            this.skip = skip;
        }
        
        public long getPosition(Pager pager)
        {
            return pager.adjust(moveNode, position, skip);
        }
    }
    
    interface MoveRecorder
    {
        public boolean involves(long position);

        public boolean record(Move move, boolean moved);
    }
     
    public final static class NullMoveRecorder
    implements MoveRecorder
    {
        public boolean involves(long position)
        {
            return false;
        }

        public boolean record(Move move, boolean moved)
        {
            return moved;
        }
    }
   
    static class CompositeMoveRecorder
    implements MoveRecorder
    {
        private final List<MoveRecorder> listOfMoveRecorders;
        
        public CompositeMoveRecorder()
        {
            this.listOfMoveRecorders = new ArrayList<MoveRecorder>();
        }
        
        public void add(MoveRecorder recorder)
        {
            listOfMoveRecorders.add(recorder);
        }
        
        public boolean involves(long position)
        {
            for (MoveRecorder recorder: listOfMoveRecorders)
            {
                if (recorder.involves(position))
                {
                    return true;
                }
            }
            return false;
        }
        
        public boolean record(Move move, boolean moved)
        {
            for (MoveRecorder recorder: listOfMoveRecorders)
            {
                moved = recorder.record(move, moved);
            }
            return moved;
        }
    }
    
    final static class SetRecorder
    extends TreeSet<Long>
    implements MoveRecorder
    {
        private static final long serialVersionUID = 1L;

        public boolean involves(long position)
        {
            return contains(position) || contains(-position);
        }
        
        public boolean record(Move move, boolean moved)
        {
            if (contains(move.getFrom()))
            {
                moved = true;
            }
            if (contains(-move.getFrom()))
            {
                remove(-move.getFrom());
                add(move.getFrom());
            }
            return moved;
        }
    }
    
    final static class MapRecorder
    extends TreeMap<Long, Movable>
    implements MoveRecorder
    {
        private static final long serialVersionUID = 1L;

        public boolean involves(long position)
        {
            return containsKey(position) || containsKey(-position);
        }
        
        public boolean record(Move move, boolean moved)
        {
            if (containsKey(move.getFrom()))
            {
                put(move.getTo(), remove(move.getFrom()));
                moved = true;
            }
            if (containsKey(-move.getFrom()))
            {
                put(move.getFrom(), remove(-move.getFrom()));
            }
            return moved;
        }
    }

    final static class PageRecorder
    extends CompositeMoveRecorder
    {
        private final Set<Long> setOfAddressPages;

        private final SetRecorder setOfUserPages;

        private final SetRecorder setOfJournalPages;
        
        private final SetRecorder setOfWritePages;
        
        private final SetRecorder setOfAllocationPages;
        
        public PageRecorder()
        {
            this.setOfAddressPages = new HashSet<Long>();
            add(this.setOfUserPages = new SetRecorder());
            add(this.setOfJournalPages = new SetRecorder());
            add(this.setOfWritePages = new SetRecorder());
            add(this.setOfAllocationPages = new SetRecorder());
        }
        
        public Set<Long> getAddressPageSet()
        {
            return setOfAddressPages;
        }
        
        public Set<Long> getUserPageSet()
        {
            return setOfUserPages;
        }
        
        public Set<Long> getJournalPageSet()
        {
            return setOfJournalPages;
        }
        
        public Set<Long> getWritePageSet()
        {
            return setOfWritePages;
        }
        
        public Set<Long> getAllocationPageSet()
        {
            return setOfAllocationPages;
        }
    }

    final static class JournalRecorder
    implements MoveRecorder
    {
        private final Journal journal;
        
        public JournalRecorder(Journal journal)
        {
            this.journal = journal;
        }

        public boolean involves(long position)
        {
            return false;
        }
        
        public boolean record(Move move, boolean moved)
        {
            if (moved)
            {
                journal.write(new ShiftMove());
            }

            return moved;
        }
    }
    
    final static class MoveNodeRecorder
    implements MoveRecorder
    {
        private final MoveNode firstMoveNode;

        private MoveNode moveNode;
        
        public MoveNodeRecorder(MoveNode moveNode)
        {
            this.firstMoveNode = moveNode;
            this.moveNode = moveNode;
        }

        public MoveNode getFirstMoveNode()
        {
            return firstMoveNode;
        }

        public MoveNode getMoveNode()
        {
            return moveNode;
        }
        
        public boolean involves(long position)
        {
            return false;
        }
        
        public boolean record(Move move, boolean moved)
        {
            if (moved)
            {
                moveNode = moveNode.extend(move);
            }
            return moved;
        }
    }
    
    private static class Journal
    {
        private final Pager pager;
        
        private final PageRecorder pageRecorder;

        private final DirtyPageMap dirtyPages;

        private final Movable journalStart;

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
            this.journalPage = pager.newInterimPage(new JournalPage(), dirtyPages);
            this.journalStart = new Movable(moveNode, journalPage.getJournalPosition(), 0);
            this.pager = pager;
            this.dirtyPages = dirtyPages;
            this.pageRecorder = pageRecorder;
            this.pageRecorder.getJournalPageSet().add(journalPage.getRawPage().getPosition());
        }
        
        public Movable getJournalStart()
        {
            return journalStart;
        }
        
        public long getJournalPosition()
        {
            return journalPage.getJournalPosition();
        }

        public void write(Operation operation)
        {
            if (!journalPage.write(operation, NEXT_PAGE_SIZE, dirtyPages))
            {
                JournalPage nextJournalPage = pager.newInterimPage(new JournalPage(), dirtyPages);
                journalPage.write(new NextOperation(nextJournalPage.getJournalPosition()), 0, dirtyPages);
                journalPage = nextJournalPage;
                pageRecorder.getJournalPageSet().add(journalPage.getRawPage().getPosition());
                write(operation);
            }
        }
    }

    private final static class Player
    {
        private final Pager pager;

        private final Pointer header;

        private long entryPosition;

        private final DirtyPageMap dirtyPages;
        
        private final Set<AddVacuum> setOfVacuums; 
        
        private final LinkedList<Move> listOfMoves;
        
        public Player(Pager pager, Pointer header, DirtyPageMap dirtyPages)
        {
            ByteBuffer bytes = header.getByteBuffer();
            
            bytes.clear();
            
            this.pager = pager;
            this.header = header;
            this.entryPosition = bytes.getLong();
            this.listOfMoves = new LinkedList<Move>();
            this.setOfVacuums = new HashSet<AddVacuum>();
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
        
        public Set<AddVacuum> getVacuumSet()
        {
            return setOfVacuums;
        }
        
        public long adjust(long position)
        {
            return pager.adjust(getMoveList(), position);
        }

        private void execute()
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
        }

        public void vacuum()
        {
            execute();
        }

        public void commit()
        {
            execute();

            header.getByteBuffer().clear();
            header.getByteBuffer().putLong(0, 0L);

            dirtyPages.flush(header);

            pager.getJournalHeaderSet().free(header);
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
        private long from;
        
        private long to;
        
        private long checksum;
        
        private int offset;
        
        public AddVacuum()
        {
        }
        
        public AddVacuum(Mirror mirror, BlockPage to)
        {
            this.from = mirror.getMirrored().getRawPage().getPosition();
            this.to = to.getRawPage().getPosition();
            this.checksum = mirror.getChecksum();
            this.offset = mirror.getOffset();
        }
        
        public void vacuum(Player player)
        {
            BlockPage mirrored = player.getPager().getPage(from, new BlockPage(false));
            BlockPage user = player.getPager().getPage(to, new BlockPage(false));
            mirrored.compact(user, player.getDirtyPages(), offset, checksum);
        }

        @Override
        public void commit(Player player)
        {
            player.getVacuumSet().add(this);
        }
        
        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 3 + COUNT_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(ADD_VACUUM);
            bytes.putLong(from);
            bytes.putLong(to);
            bytes.putLong(checksum);
            bytes.putInt(offset);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            this.from = bytes.getLong();
            this.to = bytes.getLong();
            this.checksum = bytes.getLong();
            this.offset = bytes.getInt();
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
            for (AddVacuum addVacuum: player.getVacuumSet())
            {
                addVacuum.vacuum(player);
            }
            // FIXME Prettify.
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

    private final static class CreateAddressPage
    extends Operation
    {
        private long position;
        
        public CreateAddressPage()
        {            
        }
        
        public CreateAddressPage(long position)
        {
            this.position = position;
        }
        
        @Override
        public void commit(Player player)
        {
            player.getPager().setPage(position, new AddressPage(), player.getDirtyPages(), true);
        }
        
        @Override
        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(CREATE_ADDRESS_PAGE);
            bytes.putLong(position);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            position = bytes.getLong(); 
        }
    }
    
    final static class Write
    extends Operation
    {
        private long address;
        
        private long from;
        
        public Write()
        {
        }
        
        public Write(long address, long from)
        {
            this.address = address;
            this.from = from;
        }
        
        @Override
        public void commit(Player player)
        {
            Pager pager = player.getPager();
            BlockPage interim = pager.getPage(from, new BlockPage(true));
            interim.write(address, player.getDirtyPages());
        }
        
        @Override
        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE + POSITION_SIZE;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(WRITE);
            bytes.putLong(address);
            bytes.putLong(from);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            address = bytes.getLong();
            from = bytes.getLong();
        }
    }

    final static class Free
    extends Operation
    {
        private long address;
        
        private long position;
        
        public Free()
        {
        }

        public Free(long address, long position)
        {
            this.address = address;
            this.position = position;
        }

        @Override
        public void commit(Player player)
        {
            Pager pager = player.getPager();
            AddressPage addresses = pager.getPage(address, new AddressPage());
            addresses.free(address, player.getDirtyPages());
            BlockPage blocks = pager.getPage(player.adjust(position), new BlockPage(false));
            pager.getFreePageBySize().reserve(blocks);
            blocks.free(address, player.getDirtyPages());
            pager.getFreePageBySize().release(blocks.getRawPage().getPosition());
            pager.returnUserPage(blocks);
        }

        @Override
        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE + POSITION_SIZE;
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(FREE);
            bytes.putLong(address);
            bytes.putLong(position);
        }

        @Override
        public void read(ByteBuffer bytes)
        {
            address = bytes.getLong();
            position = bytes.getLong();
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
            journalPage = player.getPager().getPage(player.adjust(position), new JournalPage());
            journalPage.seek(player.adjust(position));
            return journalPage;
        }

        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE;
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
    }

    private final static class Copy
    extends Operation
    {
        private long address;

        private long from;
        
        private long to;
        
        public Copy()
        {
        }

        public Copy(long address, long from, long to)
        {
            this.address = address;
            this.from = from;
            this.to = to;
        }
        
        @Override
        public void commit(Player player)
        {
            BlockPage interim = player.getPager().getPage(from, new BlockPage(true));
            BlockPage user = player.getPager().getPage(to, new BlockPage(false));
            interim.commit(address, user, player.getDirtyPages());
        }

        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 3;
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(COPY);
            bytes.putLong(address);
            bytes.putLong(from);
            bytes.putLong(to);
        }

        @Override
        public void read(ByteBuffer bytes)
        {
            this.address = bytes.getLong();
            this.from = bytes.getLong();
            this.to = bytes.getLong();
        }
    }

    private final static class Terminate
    extends Operation
    {
        @Override
        public boolean terminate()
        {
            return true;
        }

        @Override
        public int length()
        {
            return FLAG_SIZE;
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

    public final static class Mutator
    {
        final Pager pager;
        
        private final Journal journal;

        private final BySizeTable allocPagesBySize;
        
        private final BySizeTable writePagesBySize;

        private final SortedMap<Long, Movable> mapOfAddresses;

        private final DirtyPageMap dirtyPages;
        
        private final MoveNodeRecorder moveNodeRecorder;
        
        private final PageRecorder pageRecorder;
        
        private final MoveList listOfMoves;
        
        private long lastPointerPage;

        public Mutator(Pager pager, MoveList listOfMoves, PageRecorder pageRecorder, Journal journal, MoveNode moveNode, DirtyPageMap dirtyPages)
        {
            MoveNodeRecorder moveNodeRecorder = new MoveNodeRecorder(moveNode);
            CompositeMoveRecorder moveRecorder = new CompositeMoveRecorder();
            moveRecorder.add(pageRecorder);
            moveRecorder.add(moveNodeRecorder);
            moveRecorder.add(new JournalRecorder(journal));
            this.pager = pager;
            this.journal = journal;
            this.allocPagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.writePagesBySize = new BySizeTable(pager.getPageSize(), pager.getAlignment());
            this.dirtyPages = dirtyPages;
            this.mapOfAddresses = new TreeMap<Long, Movable>();
            this.moveNodeRecorder = moveNodeRecorder;
            this.listOfMoves = new MoveList(moveRecorder, listOfMoves);
            this.pageRecorder = pageRecorder;
        }
        
        // FIXME Make part of pack and not mutator.
        public long getStaticPageAddress(URI uri)
        {
            return pager.getStaticPageAddress(uri);
        }
        
        public long temporary(int blockSize)
        {
            throw new UnsupportedOperationException();
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
                    
            final int fullSize = blockSize + BLOCK_HEADER_SIZE;
           
            return listOfMoves.mutate(new GuardedReturnable<Long>()
            {
                public Long run(List<MoveLatch> listOfMoves)
                {
                    
                    // This is unimplemented: Creating a linked list of blocks
                    // when the block size exceeds the size of a page.
                    
                    int pageSize = pager.getPageSize();
                    if (fullSize + BLOCK_PAGE_HEADER_SIZE > pageSize)
                    {
                        // Recurse.
                        throw new UnsupportedOperationException();
                    }
                    
                    // If we already have a wilderness data page that will fit
                    // the block, use that page. Otherwise, allocate a new
                    // wilderness data page for allocation.
                    
                    BlockPage interim = null;
                    long bestFit = allocPagesBySize.bestFit(fullSize);
                    if (bestFit == 0L)
                    {
                        interim = pager.newInterimPage(new BlockPage(true), dirtyPages);
                        pageRecorder.getAllocationPageSet().add(interim.getRawPage().getPosition());
                    }
                    else
                    {
                        interim = pager.getPage(bestFit, new BlockPage(true));
                    }
                    
                    // Allocate a block from the wilderness data page.
                    
                    interim.allocate(address, fullSize, dirtyPages);
                    
                    allocPagesBySize.add(interim);
                    
                    mapOfAddresses.put(-address, new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0));
                    
                    return address;
                }
            });
        }

        private BlockPage dereference(long address, List<MoveLatch> listOfMoveLatches)
        {
            AddressPage addresses = pager.getPage(address, new AddressPage());

            long position = addresses.dereference(address);
            if (position == 0L || position == Long.MAX_VALUE)
            {
                throw new Danger(ERROR_FREED_FREE_ADDRESS);
            }
            
            for (MoveLatch latch: listOfMoveLatches)
            {
                if (latch.getMove().getFrom() == position)
                {
                    latch.enter();
                    position = latch.getMove().getTo();
                }
            }
        
            return pager.getPage(position, new BlockPage(false));
        }

        public void write(final long address, final ByteBuffer src)
        {
            listOfMoves.mutate(new Guarded()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    // For now, the first test will write to an allocated block, so
                    // the write buffer is already there.
                    BlockPage interim = null;
                    Movable position = mapOfAddresses.get(address);
                    if (position == null)
                    {
                        position = mapOfAddresses.get(-address);
                    }
                    if (position == null)
                    {
                        BlockPage blocks = dereference(address, listOfMoveLatches);
                        int blockSize = blocks.getSize(address);
                       
                        long bestFit = writePagesBySize.bestFit(blockSize);
                        if (bestFit == 0L)
                        {
                            interim = pager.newInterimPage(new BlockPage(true), dirtyPages);
                            pageRecorder.getWritePageSet().add(interim.getRawPage().getPosition());
                        }
                        else
                        {
                            interim = pager.getPage(bestFit, new BlockPage(true));
                        }
                        
                        interim.allocate(address, blockSize, dirtyPages);
                        
                        if (blockSize < src.remaining() + BLOCK_HEADER_SIZE)
                        {
                            ByteBuffer copy = ByteBuffer.allocateDirect(blockSize - BLOCK_HEADER_SIZE);
                            if (!blocks.read(address, copy))
                            {
                                throw new IllegalStateException();
                            }
                            copy.flip();
                            interim.write(address, copy, dirtyPages);
                        }

                        position = new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0);
                        mapOfAddresses.put(address, position);
                    }
                    else
                    {
                        interim = pager.getPage(position.getPosition(pager), new BlockPage(true));
                    }
        
                    if (!interim.write(address, src, dirtyPages))
                    {
                        throw new IllegalStateException();
                    }
                }
            });
        }
        
        public ByteBuffer read(final long address)
        {
            throw new UnsupportedOperationException();
        }

        public void read(final long address, final ByteBuffer bytes)
        {
            listOfMoves.mutate(new Guarded()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    Movable movable = mapOfAddresses.get(address);
                    if (movable == null)
                    {
                        movable = mapOfAddresses.get(-address);
                    }
                    if (movable == null)
                    {
                        AddressPage addresses = pager.getPage(address, new AddressPage());
                        long lastPosition = 0L;
                        for (;;)
                        {
                            long actual = addresses.dereference(address);
                            if (actual == 0L || actual == Long.MAX_VALUE)
                            {
                                throw new Danger(ERROR_READ_FREE_ADDRESS);
                            }

                            if (actual != lastPosition)
                            {
                                BlockPage blocks = pager.getPage(actual, new BlockPage(false));
                                if (blocks.read(address, bytes))
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
                        BlockPage blocks = pager.getPage(movable.getPosition(pager), new BlockPage(true));
                        blocks.read(address, bytes);
                    }
                }
            });
        }

        public void free(final long address)
        {
            if (pager.isStaticPageAddress(address))
            {
                throw new Danger(ERROR_FREED_STATIC_ADDRESS);
            }
            listOfMoves.mutate(new Guarded()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    BlockPage blocks = dereference(address, listOfMoveLatches);
                    long position = blocks.getRawPage().getPosition();
                    journal.write(new Free(address, position));
                    pageRecorder.getUserPageSet().add(position);
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
                AddressPage addresses = pager.getPage(-address, new AddressPage());
                addresses.free(-address, dirtyPages);
                dirtyPages.flushIfAtCapacity();
            }
            
            dirtyPages.flush();
            
            pager.getFreeInterimPages().free(pageRecorder.getAllocationPageSet());
            pager.getFreeInterimPages().free(pageRecorder.getWritePageSet());
            pager.getFreeInterimPages().free(pageRecorder.getJournalPageSet());
        }
  
        public void rollback()
        {
            pager.getCompactLock().readLock().lock();
            try
            {
                listOfMoves.mutate(new Guarded()
                {
                    @Override
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
        }
        
        private int getUserPageCount()
        {
            long userPageSize = pager.getInterimBoundary().getPosition()
                              - pager.getUserBoundary().getPosition();
            return (int) (userPageSize / pager.getPageSize());
        }
        
        // FIXME Remove try from these methods. It's confusing.
        private void tryExpandUser(MoveList listOfMoves, Commit commit, MoveLatch userMoves, int count)
        {
            // This invocation is to flush the move list for the current
            // mutator. You may think that this is pointless, but it's
            // not. It will ensure that the relocatable references are
            // all up to date before we try to move.
            
            // If any of the pages we currently referenced are moving
            // those moves will be complete when this call returns.
            
            listOfMoves.mutate(new Guarded()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                }
            });
            
            SortedSet<Long> setOfInUse = new TreeSet<Long>();

            // Gather the interim pages that will become data pages, moving the
            // data to interim boundary.
            
            gatherPages(count, setOfInUse, commit.getGatheredSet());
            
            // If we have interim pages in use, move them.
        
            if (setOfInUse.size() != 0)
            {
                appendToMoveList(setOfInUse, userMoves);
                pager.getMoveList().add(userMoves);
                listOfMoves.skip(userMoves);
            }
        }

        private void allocateMirrors(Commit commit)
        {
            for (long position: commit.getInUseAddressSet())
            {
                BlockPage blocks = pager.getPage(position, new BlockPage(false));
                BlockPage interim = pager.newInterimPage(new BlockPage(true), dirtyPages);
                allocPagesBySize.add(interim.getRawPage().getPosition(), blocks.getRemaining());
                pageRecorder.getAllocationPageSet().add(interim.getRawPage().getPosition());
                commit.getAddressMirrorMap().put(blocks.getRawPage().getPosition(), new Movable(moveNodeRecorder.getMoveNode(), interim.getRawPage().getPosition(), 0));
            }
        }

        private SortedSet<Long> tryNewAddressPage(MoveList listOfMoves, final Commit commit, int count)
        {
            final MoveLatch userMoves = new MoveLatch(false);
            final SortedSet<Long> setOfGathered = new TreeSet<Long>();

            pager.getExpandLock().lock();
            try
            {
                // If there is enough or else we'll wait for there to
                // be enough because an interim region expansion is ahead of
                // us in the move this. The addresses will cause us to wait.
                
                int userPageCount = getUserPageCount();
                if (userPageCount < count)
                {
                    // Now we can try to move the pages.
                    tryExpandUser(listOfMoves,
                                  commit,
                                  userMoves,
                                  count - userPageCount);
                }
            }
            finally
            {
                pager.getExpandLock().unlock();
            }
            
            if (userMoves.getNext() != null)
            {
                listOfMoves.mutate(new Guarded()
                {
                    @Override
                    public void run(List<MoveLatch> listOfMoveLatches)
                    {
                        moveAndUnlatch(userMoves);
                    }
                });
            }
            
            // Now we know we have enough user pages to accommodate our
            // creation of address pages.
            
            // Some of those user  block pages may not yet exist. We are going to 
            // have to wait until they exist before we do anything with
            // with the block pages.

            for (int i = 0; i < count; i++)
            {
                long position = pager.getUserBoundary().getPosition();
                commit.getAddressSet().add(position);
                if (!setOfGathered.contains(position))
                {
                    BlockPage blocks = pager.getPage(position, new BlockPage(false));
                    if (!pager.getFreePageBySize().reserve(blocks))
                    {
                        if (!pager.getFreeUserPages().reserve(position))
                        {
                            commit.getInUseAddressSet().add(position);
                        }
                    }
                    else
                    {
                        pager.getFreeUserPages().reserve(position);
                    }
                }
                pager.getUserBoundary().increment();
            }

            // To move a data page to make space for an address page, we simply
            // copy over the block pages that need to move, verbatim into an
            // interim block page and create a commit. The block pages will
            // operate as allocations, moving into some area within the user
            // region. The way that journals are written, vacuums and commits
            // take place before the operations written, so we write out our
            // address page initializations now.
            
            for (long position : commit.getAddressSet())
            {
                journal.write(new CreateAddressPage(position));
            }
            
            // If the new address page is in the set of free block pages or if
            // it is a block page we've just created the
            // page does not have to be moved.

            listOfMoves.mutate(new Guarded()
            {
                public void run(List<MoveLatch> listOfMoveLatches)
                {
                    allocateMirrors(commit);
                }
            });

            tryCommit(listOfMoves, commit);
            
            return commit.getAddressSet();
        }

        public SortedSet<Long> newAddressPage()
        {
            pager.getCompactLock().readLock().lock();
            
            try
            {
                final Commit commit = new Commit(pageRecorder, journal, moveNodeRecorder);
                return tryNewAddressPage(new MoveList(commit, listOfMoves), commit, 1); 
            }
            finally
            {
                pager.getCompactLock().readLock().unlock();
            }
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
         * @param setOfGathered
         *            A set of the newly created data positions.
         */
        private void gatherPages(int count, Set<Long> setOfInUse, SortedSet<Long> setOfGathered)
        {
            for (int i = 0; i < count; i++)
            {
                // Use the page that is at the data to interim boundary.
                
                long soonToBeCreatedUser = pager.getInterimBoundary().getPosition();
                
                // If it is not in the set of free pages it needs to be moved,
                // so we add it to the set of in use.
                
                if (!pager.getFreeInterimPages().reserve(soonToBeCreatedUser))
                {
                    setOfInUse.add(soonToBeCreatedUser);
                }

                // Synapse: What if this page is already in our set? That's
                // fine because we'll first check to see if the page exists
                // as a positive value, then we'll adjust the negative value.
                
                // However, I'm pretty sure that this is never the case, so
                // I'm going to put an assertion here and think about it.
                
                if (pageRecorder.getUserPageSet().contains(soonToBeCreatedUser))
                {
                    throw new IllegalStateException();
                }

                setOfGathered.add(soonToBeCreatedUser);
                
                // Increment the data to interim boundary.

                pager.getInterimBoundary().increment();
            }
        }
        
        private Map<Long, Long> associate(Commit commit, SortedSet<Long> setOfUnassigned)
        {
            SortedSet<Long> setOfGathered = new TreeSet<Long>(commit.getGatheredSet());
            Map<Long, Long> mapOfCopies = new TreeMap<Long, Long>();
            while (setOfUnassigned.size() != 0)
            {
                long interimAllocation = setOfUnassigned.first();
                setOfUnassigned.remove(interimAllocation);
                
                long soonToBeCreatedUser = setOfGathered.first();
                setOfGathered.remove(soonToBeCreatedUser);
                
                mapOfCopies.put(interimAllocation, soonToBeCreatedUser);
            }
            return mapOfCopies;
        }
        
        /**
         * Iterate the linked list of moves and move latches moving the pages
         * and then releasing the latches.
         *
         * @param head The head a linked list of move latches.
         */
        private void moveAndUnlatch(MoveLatch head)
        {
            // Iterate through the linked list moving pages and releasing
            // latches.

            while (head.getNext() != null && !head.getNext().isTerminal())
            {
                // Goto the next node.

                head = head.getNext();
                
                // Get a relocatable page.

                RelocatablePage page = pager.getPage(head.getMove().getFrom(), new RelocatablePage());
                
                // Please note that relocate simply moves the entire page with
                // an unforced write. There is minimal benefit to the dirty page
                // map since we are writing out the whole page anyway, and
                // because were only going to do a little writing after this, so
                // the chances of caching the dirty page are slim.
                
                page.relocate(head.getMove().getTo());
                
                // Anyone referencing the page is going to be waiting for the
                // move to complete, because of the move list. This goes for all
                // readers and writers who might need to dereference an interim
                // page. The mutators holding the interim pages will wait
                // regardless of whether or not they intend to manipulate the
                // pages. Note that read only mutator reading only from the
                // user region will not wait, since it will not have interim
                // pages.

                // Therefore, we can relocate these interim pages confident that
                // no one is currently attempting to dereference them.
                
                pager.relocate(head.getMove().getFrom(), head.getMove().getTo());

                pager.setPage(head.getMove().getFrom(), new BlockPage(false), dirtyPages, false);

                // Now we can let anyone who is waiting on this interim page
                // through.
                
                head.unlatch();
            }
        }
        
        private void appendToMoveList(SortedSet<Long> setOfInUse, MoveLatch head)
        {
            // For the set of pages in use, add the page to the move list.
      
            Iterator<Long> inUse = setOfInUse.iterator();
      
            // Append of the rest of the moves and latches to the list. 
      
            while (inUse.hasNext())
            {
                long from = inUse.next();
                long to = pager.newBlankInterimPage();
                if (setOfInUse.contains(to))
                {
                    throw new IllegalStateException();
                }
                head.getLast().extend(new MoveLatch(new Move(from, to), false));
            }
        }

        private void asssignAllocations(Commit commit, SortedSet<Long> setOfUnassigned)
        {
            Map<Long, Long> mapOfCopies = associate(commit, setOfUnassigned);
            
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
        private void tryExpandAddress(Commit commit, MoveLatch addressMoves)
        {
            Map<Long, Long> mapOfCopies = new TreeMap<Long, Long>();
            for (Map.Entry<Long, Movable> entry: commit.getAddressMirrorMap().entrySet())
            {
                long soonToBeCreatedAddress = entry.getKey();
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
                mapOfCopies.put(soonToBeCreatedAddress, newOrExistingUser);
            }
            
            for (Map.Entry<Long, Long> entry: mapOfCopies.entrySet())
            {
                long soonToBeCreatedAddress = entry.getKey();
                long newOrExistingUser = entry.getValue();
                addressMoves.getLast().extend(new MoveLatch(new Move(soonToBeCreatedAddress, newOrExistingUser), true));
            }
        }

        private void mirrorUsers(Commit commit, Set<BlockPage> setOfMirroredBlockPages)
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
                
                BlockPage mirrored = pager.getPage(allocation, new BlockPage(false));
                
                BlockPage blocks = pager.getPage(soonToBeCreatedAddress, new BlockPage(true));
                blocks.mirror(pager, mirrored, true, dirtyPages);

                setOfMirroredBlockPages.add(blocks);
            }
        }

        private void journalCommits(Map<Long, Movable> mapOfCommits)
        {
            for (Map.Entry<Long, Movable> entry: mapOfCommits.entrySet())
            {
                BlockPage interim = pager.getPage(entry.getKey(), new BlockPage(true));
                for (long address: interim.getAddresses())
                {
                    journal.write(new Copy(address, entry.getKey(), entry.getValue().getPosition(pager)));
                }
            }
        }

        private void tryCommit(MoveList listOfMoves, final Commit commit)
        {
            final SortedSet<Long> setOfUnassigned = new TreeSet<Long>(pageRecorder.getAllocationPageSet());

            if (setOfUnassigned.size() != 0)
            {
                // First we mate the interim data pages with 
                listOfMoves.mutate(new Guarded()
                {
                    public void run(List<MoveLatch> listOfMoveLatches)
                    {
                        // Consolidate pages by using existing, partially filled
                        // pages to store our new block allocations.
        
                        pager.getFreePageBySize().join(allocPagesBySize, pageRecorder.getUserPageSet(), commit.getVacuumMap(), moveNodeRecorder.getMoveNode());
                        setOfUnassigned.removeAll(commit.getVacuumMap().keySet());
                        
                        // Use free data pages to store the interim pages whose
                        // blocks would not fit on an existing page.
                        
                        pager.newUserPages(setOfUnassigned, pageRecorder.getUserPageSet(), commit.getEmptyMap(), moveNodeRecorder.getMoveNode());
                    }
                });
            }
        
            // If more pages are needed, then we need to extend the user area of
            // the file.
            
            final MoveLatch userMoves = new MoveLatch(false);
        
            if (setOfUnassigned.size() != 0)
            {
                pager.getExpandLock().lock();
                try
                {
                    // Now we can try to move the pages.
                    tryExpandUser(listOfMoves,
                                  commit,
                                  userMoves,
                                  setOfUnassigned.size());
                    asssignAllocations(commit, setOfUnassigned);
                }
                finally
                {
                    pager.getExpandLock().unlock();
                }
            }
            
            if (userMoves.getNext() != null)
            {
                listOfMoves.mutate(new Guarded()
                {
                    @Override
                    public void run(List<MoveLatch> listOfMoveLatches)
                    {
                        moveAndUnlatch(userMoves);
                    }
                });
            }
            
            final MoveLatch addressMoves = new MoveLatch(true);
            
            if (commit.isAddressExpansion())
            {
                listOfMoves.mutate(new Guarded()
                {
                    @Override
                    public void run(List<MoveLatch> listOfMoveLatches)
                    {
                        tryExpandAddress(commit,addressMoves);
                    }
                });

                pager.getMoveList().add(addressMoves);
                listOfMoves.skip(addressMoves);
            }

            listOfMoves.mutate(new Guarded()
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
                    Set<BlockPage> setOfMirroredBlockPages = new HashSet<BlockPage>();
                    
                    // FIXME Do I make sure that mirroring in included before 
                    // vacuum in recovery as well?
                    // FIXME No. Just make addresses go first. Negative journal.

                    for (Map.Entry<Long, Movable> entry: commit.getVacuumMap().entrySet())
                    {
                        BlockPage page = pager.getPage(entry.getValue().getPosition(pager), new BlockPage(false));
                        Mirror mirror = page.mirror(pager, null, false, dirtyPages);
                        if (mirror != null)
                        {
                            journal.write(new AddVacuum(mirror, page));
                            setOfMirroredBlockPages.add(page);
                        }
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
                    
                    mirrorUsers(commit, setOfMirroredBlockPages);

                    journalCommits(commit.getVacuumMap());
                    journalCommits(commit.getEmptyMap());
                   
                    for (long position: pageRecorder.getWritePageSet())
                    {
                        BlockPage page = pager.getPage(position, new BlockPage(true));
                        for (long address: page.getAddresses())
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
                    
                    if (!commit.isAddressExpansion())
                    {
                        setOfMirroredBlockPages.clear();
                    }

                    // Then do everything else.
                    player.commit();

                    setOfMirroredBlockPages.clear();
                    
                    if (!commit.isAddressExpansion())
                    {
                        pager.getFreeInterimPages().free(commit.getVacuumMap().keySet());
                        pager.getFreeInterimPages().free(commit.getEmptyMap().keySet());
                        for (Map.Entry<Long, Movable> entry: commit.getVacuumMap().entrySet())
                        {
                            pager.returnUserPage(pager.getPage(entry.getValue().getPosition(pager), new BlockPage(false)));
                        }
                        for (Map.Entry<Long, Movable> entry: commit.getEmptyMap().entrySet())
                        {
                            pager.returnUserPage(pager.getPage(entry.getValue().getPosition(pager), new BlockPage(false)));
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
            while (iterator.getNext() != null && !iterator.getNext().isTerminal())
            {
                iterator.getNext().unlatch();
                iterator = iterator.getNext();
            }
        }

        public void commit()
        {
            pager.getCompactLock().readLock().lock();
            try
            {
                final Commit commit = new Commit(pageRecorder, journal, moveNodeRecorder);
                tryCommit(new MoveList(commit, listOfMoves), commit);
            }
            finally
            {
                pager.getCompactLock().readLock().unlock();
            }
        }
    }
    
    final static class Commit
    extends CompositeMoveRecorder
    {
        private final MapRecorder mapOfVaccums;
        
        private final MapRecorder mapOfEmpties;
        
        private final SortedSet<Long> setOfAddressPages;
        
        private final SortedSet<Long> setOfGatheredPages;
        
        private final SortedSet<Long> setOfInUseAddressPages;
        
        private final SortedMap<Long, Movable> mapOfAddressMirrors;
        
        public Commit(PageRecorder pageRecorder, Journal journal, MoveNodeRecorder moveNodeRecorder)
        {
            this.setOfAddressPages = new TreeSet<Long>();
            this.setOfGatheredPages = new TreeSet<Long>();
            this.setOfInUseAddressPages = new TreeSet<Long>();
            this.mapOfAddressMirrors = new TreeMap<Long, Movable>();
            add(pageRecorder);
            add(mapOfVaccums = new MapRecorder());
            add(mapOfEmpties = new MapRecorder());
            add(moveNodeRecorder);
            add(new JournalRecorder(journal));
        }
        
        @Override
        public boolean involves(long position)
        {
            return setOfAddressPages.contains(position)
                || super.involves(position);
        }
        
        public boolean isAddressExpansion()
        {
            return setOfAddressPages.size() != 0;
        }

        public SortedSet<Long> getAddressSet()
        {
            return setOfAddressPages;
        }
        
        public SortedSet<Long> getGatheredSet()
        {
            return setOfGatheredPages;
        }
        
        public SortedMap<Long, Movable> getAddressMirrorMap()
        {
            return mapOfAddressMirrors;
        }
        
        public SortedSet<Long> getInUseAddressSet()
        {
            return setOfInUseAddressPages;
        }
        
        public SortedMap<Long, Movable> getVacuumMap()
        {
            return mapOfVaccums;
        }
        
        public SortedMap<Long, Movable> getEmptyMap()
        {
            return mapOfEmpties;
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
