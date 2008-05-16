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

    private final static int ADDRESS_SIZE = 8;

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

    private final static short COMMIT = 9;

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
            
            long position = (pager.getDataBoundary().getPosition() - 1) / pager.getPageSize() * pager.getPageSize();
            
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
                pager.getDataBoundary().increment();
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

        private final Boundary dataBoundary;
        
        private final Boundary interimBoundary;
            
        private final MoveList listOfMoves;

        private final SortedSet<Long> setOfAddressPages;
        
        private final Set<Long> setOfReturningAddressPages;
        
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
        
        /**
         * A read/write lock that coordinates rewind of area boundaries and the
         * wilderness. 
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
            this.dataBoundary = new Boundary(pageSize, dataBoundary);
            this.interimBoundary = new Boundary(pageSize, interimBoundary);
            this.mapOfPagesByPosition = new HashMap<Long, PageReference>();
            this.pagesBySize = new BySizeTable(pageSize, alignment);
            this.mapOfStaticPages = mapOfStaticPages;
            this.setOfFreeUserPages = new TreeSet<Long>();
            this.setOfFreeInterimPages = new TreeSet<Long>(new Reverse<Long>());
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
        
        public void returnInterimPage(long position)
        {
            synchronized (setOfFreeInterimPages)
            {
                setOfFreeInterimPages.add(position);
            }
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

        public void newUserPages(Set<Long> setOfSourcePages, Set<Long> setOfDataPages, Map<Long, MovablePosition> mapOfPages, MoveNode moveNode)
        {
            synchronized (setOfFreeUserPages)
            {
                while (setOfFreeUserPages.size() != 0 && setOfSourcePages.size() != 0)
                {
                    Iterator<Long> pages = setOfSourcePages.iterator();
                    Iterator<Long> freeUserPages = setOfFreeUserPages.iterator();
                    long position = freeUserPages.next();
                    setOfDataPages.add(position);
                    mapOfPages.put(pages.next(), new MovablePosition(moveNode, position));
                    pages.remove();
                    freeUserPages.remove();
                }
            }
        }

        public void returnUserPage(BlockPage blockPage)
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

        public boolean removeUserPageIfFree(long position)
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
                for (long position: pagesBySize)
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

        private void invalidate(long start, long end)
        {
            if (start < getPosition())
            {
                throw new IllegalStateException();
            }
            
            if (end > getPosition() + getByteBuffer().capacity())
            {
                throw new IllegalStateException();
            }
            
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

        public void invalidate(int offset, int length)
        {
            long start = getPosition() + offset;
            long end = start + length;
            invalidate(start, end);
        }
        
        public void write(Disk disk, FileChannel fileChannel) throws IOException
        {
            ByteBuffer bytes = getByteBuffer();
            bytes.clear();

            for(Map.Entry<Long, Long> entry: setOfRegions.entrySet())
            {
                int offset = (int) (entry.getKey() - getPosition());
                int length = (int) (entry.getValue() - entry.getKey());

                bytes.limit(offset + length);
                bytes.position(offset);
                
                disk.write(fileChannel, bytes, entry.getKey());
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
                bytes.putLong(offset, 0L);
                getRawPage().invalidate(offset, POSITION_SIZE);
                dirtyPages.add(getRawPage());
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
        
        private void advance(ByteBuffer bytes, int size)
        {
            bytes.position(bytes.position() + Math.abs(size));
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
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                return getBlockSize(bytes);
            }
            throw new IllegalStateException();
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
        public Mirror mirror(Pager pager, DirtyPageMap dirtyPages, boolean force)
        {
            int offset = force ? 0 : -1;
            BlockPage mirrored = null;
            
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
                        if (offset != -1)
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
                
                return null;
            }
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
                return false;
            }
        }

        public void compact(BlockPage user, DirtyPageMap dirtyPages, int offset, long checksum)
        {
            throw new UnsupportedOperationException();
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

        public void commit(long address, BlockPage page, DirtyPageMap dirtyPages)
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

                    page.commit(address, bytes.slice(), dirtyPages, getRawPage().getPosition());

                    bytes.limit(bytes.capacity());
                }
            }
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
                        || address >= pager.getDataBoundary().getPosition())
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
                case COMMIT:
                    return new Commit();
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

    final static class BySizeTable implements Iterable<Long>
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
        
        public Iterator<Long> iterator()
        {
            return new BySizeTableIterator(listOfListsOfSizes);
        }

        public synchronized void add(BlockPage blocks)
        {
            // Maybe don't round down if exact.
            int aligned = ((blocks.getRemaining() | alignment - 1) + 1) - alignment;
            if (aligned != 0)
            {
                listOfListsOfSizes.get(aligned / alignment).addFirst(blocks.getRawPage().getPosition());
            }
        }
        
        public synchronized void remove(BlockPage blocks)
        {
            // Maybe don't round down if exact.
            int aligned = ((blocks.getRemaining() | alignment - 1) + 1) - alignment;
            if (aligned != 0)
            {
                LinkedList<Long> listOfPositions = listOfListsOfSizes.get(aligned / alignment);
                listOfPositions.remove(blocks.getRawPage().getPosition());
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

    final static class Reverse<T extends Comparable<T>> implements Comparator<T>
    {
        public int compare(T left, T right)
        {
            return right.compareTo(left);
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
    
    interface Returnable<T>
    {
        public T run();
    }
   
    static final class Move
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

    static final class MoveLatch
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
            while (iterator.next == null)
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

    static class MovablePosition
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

    static final class SkippingMovablePosition
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
    
    interface MoveRecorder
    {
        public boolean contains(long position);

        public void record(Move move);
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

    final static class PageRecorder implements MoveRecorder
    {
        private final Set<Long> setOfUserPages;

        private final Set<Long> setOfJournalPages;
        
        private final Set<Long> setOfWritePages;
        
        private final Set<Long> setOfAllocationPages;
        
        public PageRecorder()
        {
            this.setOfUserPages = new HashSet<Long>();
            this.setOfJournalPages = new HashSet<Long>();
            this.setOfWritePages = new HashSet<Long>();
            this.setOfAllocationPages = new HashSet<Long>();
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
        
        public boolean contains(long position)
        {
            return setOfUserPages.contains(position)
                || setOfJournalPages.contains(position)
                || setOfWritePages.contains(position)
                || setOfAllocationPages.contains(position);
        }

        public void record(Move move)
        {
            if (setOfUserPages.remove(move.getFrom()))
            {
                setOfUserPages.add(move.getTo());
            }
            if (setOfJournalPages.remove(move.getFrom()))
            {
                setOfJournalPages.add(move.getTo());
            }
            if (setOfWritePages.remove(move.getFrom()))
            {
                setOfWritePages.add(move.getTo());
            }
            if (setOfAllocationPages.remove(move.getFrom()))
            {
                setOfAllocationPages.add(move.getTo());
            }
        }
    }

    final static class MutateMoveRecorder
    implements MoveRecorder
    {
        private final Journal journal;

        private final MoveNode firstMoveNode;

        private MoveNode moveNode;
        
        private final PageRecorder pageRecorder;

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

    final static class CommitMoveRecorder
    implements MoveRecorder
    {
        private final Journal journal;
        
        private final MoveNode firstMoveNode;

        private MoveNode moveNode;
        
        private final SortedMap<Long, MovablePosition> mapOfVacuums;

        private final SortedMap<Long, MovablePosition> mapOfEmpties;

        private final Set<Long> setOfDataPages;

        private final PageRecorder pageRecorder;

        public CommitMoveRecorder(PageRecorder pageRecorder, Journal journal, MoveNode moveNode)
        {
            this.pageRecorder = pageRecorder;
            this.journal = journal;
            this.setOfDataPages = new HashSet<Long>();
            this.mapOfVacuums = new TreeMap<Long, MovablePosition>();
            this.mapOfEmpties = new TreeMap<Long, MovablePosition>();
            this.firstMoveNode = moveNode;
            this.moveNode = moveNode;
        }
        
        public boolean contains(long position)
        {
            return pageRecorder.contains(position)
                || setOfDataPages.contains(-position)
                || setOfDataPages.contains(position)
                || mapOfVacuums.containsKey(-position)
                || mapOfVacuums.containsKey(position)
                || mapOfEmpties.containsKey(-position)
                || mapOfEmpties.containsKey(position);
        }
        
        /**
         * Record a move.
         * <p>
         * You wanted to know why you're ignoring the data page move and not
         * ignoring some of the mapped moves. It's because when you were moving
         * you were only using interim pages as keys and not inspecting them.
         * You were making no note of whether or not they were going to be
         * moved, so you've stored their pre-move value. Even if they are pages
         * that this mutator has moved, we know nothing of it. However we did
         * store the moved value of the data page, we had to. We had to store
         * the moved value of the data page. That is a move that we need to
         * observe if we've moved one our interim pages, but ignore if it's a
         * data page. It is the place where our moving started.
         */
        public void record(Move move)
        {
            boolean moved = pageRecorder.contains(move.getFrom());
            pageRecorder.record(move);
            if (mapOfVacuums.containsKey(move.getFrom()))
            {
                mapOfVacuums.put(move.getTo(), mapOfVacuums.remove(move.getFrom()));
                moved = true;
            }
            if (mapOfVacuums.containsKey(-move.getFrom()))
            {
                mapOfVacuums.put(move.getFrom(), mapOfVacuums.remove(-move.getFrom()));
            }
            if (mapOfEmpties.containsKey(move.getFrom()))
            {
                mapOfEmpties.put(move.getTo(), mapOfEmpties.remove(move.getFrom()));
                moved = true;
            }
            if (mapOfEmpties.containsKey(-move.getFrom()))
            {
                mapOfEmpties.put(move.getFrom(), mapOfEmpties.remove(-move.getFrom()));
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
            // FIXME SkippableMovablePosition? Aren't we doing that by not adding it?
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
        
        public SortedMap<Long, MovablePosition> getVacuumMap()
        {
            return mapOfVacuums;
        }
        
        public SortedMap<Long, MovablePosition> getEmptyMap()
        {
            return mapOfEmpties;
        }
        
        public Set<Long> getDataPageSet()
        {
            return setOfDataPages;
        }

        public PageRecorder getPageRecorder()
        {
            return pageRecorder;
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
            this.journalPage = pager.newInterimPage(new JournalPage(), dirtyPages);
            this.journalStart = new MovablePosition(moveNode, journalPage.getJournalPosition());
            this.pager = pager;
            this.dirtyPages = dirtyPages;
            this.pageRecorder = pageRecorder;
            this.pageRecorder.getJournalPageSet().add(journalPage.getRawPage().getPosition());
        }
        
        public MovablePosition getJournalStart()
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
            
            header.getByteBuffer().clear();
            header.getByteBuffer().putLong(0, 0L);

            dirtyPages.flush(header);

            pager.getJournalHeaderSet().free(header);
            
            assert operation instanceof Terminate;
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
            return FLAG_SIZE + POSITION_SIZE * 3;
        }
        
        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(VACUUM);
            bytes.putLong(from);
            bytes.putLong(to);
            bytes.putLong(checksum);
        }
        
        @Override
        public void read(ByteBuffer bytes)
        {
            this.from = bytes.getLong();
            this.to = bytes.getLong();
            this.checksum = bytes.getLong();
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
            Pager pager = player.getPager();
            pager.setPage(position, new AddressPage(), player.getDirtyPages(), true);
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
        
        public Free()
        {
        }

        public Free(long address)
        {
            this.address = address;
        }

        @Override
        public void commit(Player player)
        {
            Pager pager = player.getPager();
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
                        if (blocks.free(address, player.getDirtyPages()))
                        {
                            addresses.free(address, player.getDirtyPages());
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
        }

        @Override
        public int length()
        {
            return FLAG_SIZE + ADDRESS_SIZE;
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(FREE);
            bytes.putLong(address);
        }

        @Override
        public void read(ByteBuffer bytes)
        {
            address = bytes.getLong();
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

    private final static class Commit
    extends Operation
    {
        private long address;

        private long from;
        
        private long to;
        
        public Commit()
        {
        }

        public Commit(long address, long from, long to)
        {
            this.address = address;
            this.from = from;
            this.to = to;
        }
        
        @Override
        public void commit(Player player)
        {
            BlockPage interimPage = player.getPager().getPage(from, new BlockPage(true));
            BlockPage userPage = player.getPager().getPage(to, new BlockPage(false));
            interimPage.commit(address, userPage, player.getDirtyPages());
        }

        @Override
        public int length()
        {
            return FLAG_SIZE + POSITION_SIZE * 3;
        }

        @Override
        public void write(ByteBuffer bytes)
        {
            bytes.putShort(COMMIT);
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
        
        public long getStaticPageAddress(URI uri)
        {
            return pager.getStaticPageAddress(uri);
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
           
            return listOfMoves.mutate(new Returnable<Long>()
            {
                public Long run()
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
                    
                    BlockPage allocPage = null;
                    long alloc = allocPagesBySize.bestFit(fullSize);
                    if (alloc == 0L)
                    {
                        allocPage = pager.newInterimPage(new BlockPage(true), dirtyPages);
                        moveRecorder.getPageRecorder().getAllocationPageSet().add(allocPage.getRawPage().getPosition());
                    }
                    else
                    {
                        allocPage = pager.getPage(alloc, new BlockPage(true));
                    }
                    
                    // Allocate a block from the wilderness data page.
                    
                    allocPage.allocate(address, fullSize, dirtyPages);
                    
                    allocPagesBySize.add(allocPage);
                    
                    mapOfAddresses.put(address, new MovablePosition(moveRecorder.getMoveNode(), allocPage.getRawPage().getPosition()));
                    
                    return address;
                }
            });
        }

        public void write(final long address, final ByteBuffer src)
        {
            listOfMoves.mutate(new Runnable()
            {
                public void run()
                {
                    // For now, the first test will write to an allocated block, so
                    // the write buffer is already there.
                    ByteBuffer copy = null;
                    BlockPage interimPage = null;
                    MovablePosition position = mapOfAddresses.get(address);
                    if (position == null)
                    {
                        int blockSize = 0;
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
                                    if (blocks.contains(address))
                                    {
                                        pageRecorder.getUserPageSet().add(actual);
                                        blockSize = blocks.getSize(address);
                                        if (blockSize < src.remaining() + BLOCK_HEADER_SIZE)
                                        {
                                            copy = ByteBuffer.allocateDirect(blockSize - BLOCK_HEADER_SIZE);
                                            blocks.read(address, copy);
                                        }
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
                        
                        long interim = writePagesBySize.bestFit(blockSize);
                        if (interim == 0L)
                        {
                            interimPage = pager.newInterimPage(new BlockPage(true), dirtyPages);
                            moveRecorder.getPageRecorder().getWritePageSet().add(interimPage.getRawPage().getPosition());
                        }
                        else
                        {
                            interimPage = pager.getPage(interim, new BlockPage(true));
                        }
                        
                        interimPage.allocate(address, blockSize, dirtyPages);
                        
                        if (copy != null)
                        {
                            copy.flip();
                            interimPage.write(address, copy, dirtyPages);
                        }
                        
                        position = new MovablePosition(moveRecorder.getMoveNode(), interimPage.getRawPage().getPosition());
                        mapOfAddresses.put(address, position);
                    }
                    else
                    {
                        interimPage = pager.getPage(position.getValue(pager), new BlockPage(true));
                    }
        
                    if (!interimPage.write(address, src, dirtyPages))
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
            listOfMoves.mutate(new Runnable()
            {
                public void run()
                {
                    MovablePosition position = mapOfAddresses.get(address);
                    if (position == null)
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
                        BlockPage blocks = pager.getPage(position.getValue(pager), new BlockPage(true));
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
            listOfMoves.mutate(new Runnable()
            {
                public void run()
                {
                    AddressPage addressPage = pager.getPage(address, new AddressPage());
                    long lastPosition = 0L;
                    for (;;)
                    {
                        long actual = addressPage.dereference(address);
                        if (actual == 0L || actual == Long.MAX_VALUE)
                        {
                            throw new Danger(ERROR_FREED_FREE_ADDRESS);
                        }

                        if (actual != lastPosition)
                        {
                            BlockPage page = pager.getPage(actual, new BlockPage(false));
                            synchronized (page.getRawPage())
                            {
                                if (page.contains(address))
                                {
                                    journal.write(new Free(address));
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
                }
            });
        }
        
        public void rollback()
        {
            throw new UnsupportedOperationException();
        }

        private long tryNewAddressPage(int count)
        {
            SortedSet<Long> setOfAddressPages = new TreeSet<Long>();
            for (int i = 0; i < count; i++)
            {
                setOfAddressPages.add(pager.getDataBoundary().getPosition());
                pager.getDataBoundary().increment();
            }

            // To move a data page to make space for an address page, we simply
            // copy over the block pages that need to move, verbatim into an
            // interim block page and create a commit. The block pages will
            // operate as allocations, moving into some area within the user
            // region. The way that journals are written, vacuums and commits
            // take place before the operations written, so we write out our
            // address page initializations now.
            
            for (long position: setOfAddressPages)
            {
                journal.write(new CreateAddressPage(position));
            }
            
                        // The set of address pages is a set of pages that need to be
            // moved to new block pages. The data boundary has already been
            // adjusted. The pages need to be moved.
            
            // We're going to create a set of address pages to move, which 
            // is separate from the full set of address pages to initialize.
            
            final Set<Long> setOfInUseAddressPages = new HashSet<Long>(setOfAddressPages);
            
            // If the new address page is in the set of free block pages, the
            // page does not have to be moved.
            if (setOfInUseAddressPages.size() != 0)
            {
                Iterator<Long> positions = setOfInUseAddressPages.iterator();
                while (positions.hasNext())
                {
                    long position = positions.next();
                    if (pager.removeUserPageIfFree(position))
                    {
                        positions.remove();
                    }
                }
            }

            // Synapse: Deadlock if we mirror here. Must mirror while holding
            // a shared lock on the move list and must release the mirrors
            // before we release the lock. 
            
            for (long position: setOfInUseAddressPages)
            {
                // FIXME Also remove from by size map, and note somehow,
                // if we are the ones to return the pages.
                BlockPage blocks = pager.getPage(position, new BlockPage(false));
                pager.pagesBySize.remove(blocks);
                pager.removeUserPageIfFree(position);
                allocPagesBySize.add(blocks);
                pageRecorder.getAllocationPageSet().add(blocks.getRawPage().getPosition());
            }

            tryCommit(setOfInUseAddressPages);
            
            return setOfAddressPages.first();
        }

        public long newAddressPage()
        {
            pager.getCompactLock().readLock().lock();
            
            try
            {
                return tryNewAddressPage(1); 
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
        private void gatherPages(Set<Long> setOfPages,
            Set<Long> setOfMovingPages, Map<Long, MovablePosition> mapOfPages,
            Set<Long> setOfInUse, Set<Long> setOfGathered, boolean addresses)
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

                mapOfPages.put(addresses ? -from : from, new SkippingMovablePosition(moveRecorder.getMoveNode(), position));

                // Add this page to the set of new data pages.
                
                setOfGathered.add(position);

                // Increment the data to interim boundary.

                pager.getInterimBoundary().increment();
            }
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

            while (head != null)
            {
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

                // Now we can let anyone who is waiting on this interim page
                // through.
                
                head.getLock().unlock();
                
                // Onto the next page.
                
                head = head.getNext();
            }  
        }

        private MoveLatch appendToMoveList(Map<Long, MovablePosition> mapOfInUse, CommitMoveRecorder commit, MoveLatch head)
        {
            // For the set of pages in use, add the page to the move list.
      
            Iterator<Map.Entry<Long, MovablePosition>> inUse = mapOfInUse.entrySet().iterator();
      
            // Append of the rest of the moves and latches to the list. 
      
            while (inUse.hasNext())
            {
                Map.Entry<Long, MovablePosition> entry = inUse.next();
                long from = - entry.getKey();
                long to = entry.getValue().getValue(pager);
                head = new MoveLatch(new Move(from, to), head);
            }
      
            // This will append the moves to the move list.
            if (head != null)
            {
                pager.getMoveList().add(head);
            }
      
            return head;
        }

        private MoveLatch appendToMoveList(SortedSet<Long> setOfInUse)
        {
            // For the set of pages in use, add the page to the move list.
      
            MoveLatch head = null;  // Head of the list.
      
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
                head = new MoveLatch(new Move(from, to), head);
            }
      
            // This will 
            if (head != null)
            {
                pager.getMoveList().add(head);
            }
      
            return head;
        }

        private void tryMove(CommitMoveRecorder commit, boolean addresses)
        {
            SortedSet<Long> setOfInUse = new TreeSet<Long>();
            SortedSet<Long> setOfGathered = new TreeSet<Long>();
        
            // Gather the interim pages that will become data pages, moving the
            // data to interim boundary.
        
            gatherPages(pageRecorder.getAllocationPageSet(),
                        commit.getDataPageSet(),
                        commit.getEmptyMap(),
                        setOfInUse,
                        setOfGathered, addresses);
            
            // If we have interim pages in use, move them.
        
            if (setOfInUse.size() != 0)
            {
                MoveLatch head = appendToMoveList(setOfInUse);
                         
                // At this point, no one else is moving because we have a
                // monitor that only allows one mutator to move at once. Other
                // mutators may be referencing pages that are moved. Appending
                // to the move list blocked referencing mutators with a latch on
                // each move. We are clear to move the pages that are in use.
        
                moveAndUnlatch(head);
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
            // scanning through the data pages. This will represent a rough
            // area. Once we begin to see interim pages, we can still expect to
            // see data pages. We need to scan all the way to the end of the
            // file for block pages. Also, these new user pages will be empty
            // and for the purpouses of recover can be ignored.
        
            // Synapse: And if we crash now? What about these phantom new pages?
            // If we crash now, there are no journals queued because we've
            // blocked out all committers. The first journal to initialize will
            // force the new file. Until then things are wavy, but it's only
            // interim pages and empty user pages that are in play.
            
            // Synapse: Why are creating the pages here when they are temporary?
            // Can't we wait until we really need them? No. You would need to
            // keep the fact that they are uninitialized around somehow, and
            // write that into the journal. Just get it over with.
        
            for (long gathered: setOfGathered)
            {
                pager.setPage(gathered, new BlockPage(false), dirtyPages, false);
            }
        }

        private void journalMirrorCommits(Map<Long, MovablePosition> mapOfCommits, Set<BlockPage> setOfMirroredBlockPages)
        {
            for (Map.Entry<Long, MovablePosition> entry: mapOfCommits.entrySet())
            {
                long from = entry.getKey();
                long to = entry.getValue().getValue(pager);
                BlockPage blocks = pager.getPage(from, new BlockPage(true));
                Mirror mirror = blocks.mirror(pager, dirtyPages, true);
                // FIXME BEFORE I DO THIS. I need to know that their is no dirty page?
                // No. I'm copying everything.
                // Ah! But someone might flush.
                long mirrored = mirror.getMirrored().getRawPage().getPosition();
                
                // FIXME Well at this point we've blocked everyone by virtue
                // of the move list, and the writers by virtue of mirroring.
                
                // Now, when others enter, they will adjust their move lists.
                // When writers resume, they will fail.
                // If there are any unflushed writes, we have copied them
                // with the mirror.
                
                // What if someone writes before the mirror? But, we never
                // get to commit? Regardless, they could flush their 
                // write page. Or if we invalidate it, we could crash before
                // reaching commit.
                
                // Ah, can't we simply add this page to our dirty page map?
                // Then it gets flushed, if it's dirty according to itself,
                // before we begin destroying it.
                
                // In recovery, we will have to be run first, addresses are,
                // so that we can put things in place. The write will
                // find the correct place through addressing.
                
                // FIXME Wait does it matter? Someone else is committing. We'll
                // capture some of their commit. We'll replay it if we get there,
                // if we don't get there we won't. Ah, it doesn't matter for
                // playback at all, only for now, while processes are live,
                // we don't want the other thread to mistakenly flush that 
                // source page, so we'll force it before we begin to write.
                
                for (long address: mirror.getMirrored().getAddresses())
                {
                    journal.write(new Commit(address, mirrored, to));
                }

                dirtyPages.add(blocks.getRawPage());
                setOfMirroredBlockPages.add(blocks);
            }
        }

        private void journalCommits(Map<Long, MovablePosition> mapOfCommits)
        {
            for (Map.Entry<Long, MovablePosition> entry: mapOfCommits.entrySet())
            {
                BlockPage blocks = pager.getPage(entry.getKey(), new BlockPage(true));
                for (long address: blocks.getAddresses())
                {
                    journal.write(new Commit(address, entry.getKey(), entry.getValue().getValue(pager)));
                }
            }
        }

        private void tryCommit(final Set<Long> setOfInUseAddressPages)
        {
            final CommitMoveRecorder commit = new CommitMoveRecorder(pageRecorder, journal, moveRecorder.getMoveNode());

            // Moving address pages is like allocation pages. Allocation pages
            // can cause a move of interim pages because allocation can consume
            // more space, while writes and frees do not.

            // A commit does not contain both new address pages and new
            // allocation pages. We're doing one or the other, but using the
            // same ungangly method since the thoughts are all here for now. It
            // will be easier to split this out a bit later.

            // Essentially, the address region expansion is similar to the user
            // region expansion. We are doing the same thing. We are copying a
            // block page into the user region, but from where user region used
            // to be. We commit the existing data pages over to the user region
            // and they update the address pages as if it were an ordinary
            // commit. After we commit the transaction, we can proceed to build
            // the address pages in memory, only writing them out at the next
            // commit. We cannot destroy it as part of the ...
            
            // Moving back is worse then. Too confusing.

            // FIXME None of these rebuilds are actually using addressing in the
            // addressing tables. Only assigning.

            // FIXME Then recovery is try for address based recovery, but then
            // try to run journals, then rebuild address table by running any
            // journals first. Wanted to check the checksums of the block pages,
            // though, wanted to know that they were good.

            // FIXME Than read through the journals and see which block pages
            // they are touching? They will rebuild the block pages correctly at
            // least, and then we can run thourgh the block pages and rebuild
            // addressing.

            // FIXME If there are no journals, then address recovery must be
            // good.

            // FIXME Is there a difference between address journals and block
            // journals? If address journals are preseent, than we can expect
            // that things are awkward in addressing. We can replay those first,
            // because they are aggressive.

            // The alternative is to copy the data page into the interim area
            // and work our way out from there, but we're going to do it this
            // way for now.

            // For recovery, we are going to have to note that there may be
            // duplicate pages. They will checksum correctly, but the addresses
            // will point to the wrong place. This will cause us to rebuild the
            // addressing. Bad data pages will be ignored. Earlier data pages
            // will have moved 

            if (pageRecorder.getAllocationPageSet().size() != 0)
            {
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
                        
                        pager.newUserPages(pageRecorder.getAllocationPageSet(), commit.getDataPageSet(), commit.getEmptyMap(), moveRecorder.getMoveNode());
                    }
                });
            }
        
            MoveLatch addressMoves = null;
            
            final boolean addresses = setOfInUseAddressPages.size() != 0;
        
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
        
                    tryMove(commit, addresses);
                    
                    if (addresses)
                    {
                        addressMoves = appendToMoveList(commit.getVacuumMap(), commit, addressMoves);
                        addressMoves = appendToMoveList(commit.getEmptyMap(), commit, addressMoves);
                    }
                }
                finally
                {
                    pager.getExpandLock().unlock();
                }
            }
            
            // FIXME Work what you know to be true into this strategy.
            // You know that you'll only expanding here, that compacting locks
            // everyone else out. You do not have to track so viciously. 
            
            // FIXME The address moves might even deadlock, but for the 
            // fact that Lock is reentrant. But, there seems to be no way
            // for someone else to move those user pages. Only one mutator
            // at a time may expand the addresses.
            
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
                    Set<BlockPage> setOfMirroredBlockPages = new HashSet<BlockPage>();
                    
                    // FIXME Do I make sure that mirroring in included before 
                    // vacuum in recovery as well?
                    // FIXME No. Just make addresses go first. Negative journal.

                    for (Map.Entry<Long, MovablePosition> entry: commit.getVacuumMap().entrySet())
                    {
                        BlockPage page = pager.getPage(entry.getValue().getValue(pager), new BlockPage(false));
                        Mirror mirror = page.mirror(pager, dirtyPages, false);
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
                    
                    if (addresses)
                    {
                        journalMirrorCommits(commit.getVacuumMap(), setOfMirroredBlockPages);
                        journalMirrorCommits(commit.getEmptyMap(), setOfMirroredBlockPages);
                    }
                    else
                    {
                        journalCommits(commit.getVacuumMap());
                        journalCommits(commit.getEmptyMap());
                    }
                   
                    for (long position: commit.getPageRecorder().getWritePageSet())
                    {
                        BlockPage page = pager.getPage(position, new BlockPage(true));
                        for (long address: page.getAddresses())
                        {
                            journal.write(new Write(address, position));
                        }
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
                    
                    if (!addresses)
                    {
                        setOfMirroredBlockPages.clear();
                    }
                    
                    // Then do everything else.
                    player.commit();

                    setOfMirroredBlockPages.clear();
                    
                    if (!addresses)
                    {
                        for (Map.Entry<Long, MovablePosition> entry: commit.getVacuumMap().entrySet())
                        {
                            pager.returnInterimPage(entry.getKey());
//                            pager.returnUserPage(pager.getPage(entry.getValue().getValue(pager), new BlockPage(false)));
                        }
                        for (Map.Entry<Long, MovablePosition> entry: commit.getEmptyMap().entrySet())
                        {
                            pager.returnInterimPage(entry.getKey());
                            pager.returnUserPage(pager.getPage(entry.getValue().getValue(pager), new BlockPage(false)));
                        }
                    }
                    else
                    {
                        // FIXME Do I return user pages here?
                    }
                    
                    for (long position: commit.getPageRecorder().getJournalPageSet())
                    {
                        pager.returnInterimPage(position);
                    }
                    
                    for (long position: commit.getPageRecorder().getWritePageSet())
                    {
                        pager.returnInterimPage(position);
                    }
                }
            });
            
            while (addressMoves != null)
            {
                addressMoves.getLock().unlock();
                addressMoves = addressMoves.getNext();
            }
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
                tryCommit(new HashSet<Long>());
            }
            finally
            {
                pager.getCompactLock().readLock().unlock();
            }
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=80 nowrap: */
