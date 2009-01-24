package com.goodworkalan.pack;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


/**
 * Opens pack files and performs recovery.
 * 
 * TODO Pull Medic out of this class.
 * TODO Test temporary pages.
 */
public final class Opener
{
    private Disk disk;
    
    private final Set<Long> setOfTemporaryBlocks;
    
    public Opener()
    {
        this.disk = new Disk();
        this.setOfTemporaryBlocks = new HashSet<Long>();
    }
    
    public Set<Long> getTemporaryBlocks()
    {
        return setOfTemporaryBlocks;
    }
    
    public void setDisk(Disk disk)
    {
        this.disk = disk;
    }
    
    private boolean badAddress(Header header, long address)
    {
        return address < header.getFirstAddressPageStart() + Pack.ADDRESS_PAGE_HEADER_SIZE
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
            throw new PackException(Pack.ERROR_IO_READ, e);
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
                throw new PackException(Pack.ERROR_IO_STATIC_PAGES);
            }
            try
            {
                mapOfStaticPages.put(new URI(uri.toString()), address);
            }
            catch (URISyntaxException e)
            {
                throw new PackException(Pack.ERROR_IO_STATIC_PAGES, e);
            }
        }
        return mapOfStaticPages;
    }    


    private Header readHeader(FileChannel fileChannel)
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(Pack.FILE_HEADER_SIZE);
        try
        {
            disk.read(fileChannel, bytes, 0L);
        }
        catch (IOException e)
        {
           throw new PackException(Pack.ERROR_IO_READ, e);
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
            throw new PackException(Pack.ERROR_FILE_NOT_FOUND, e);
        }

        // Read the header and obtain the basic file properties.

        Header header = readHeader(fileChannel);

        if (header.getSignature() != Pack.SIGNATURE)
        {
            throw new PackException(Pack.ERROR_SIGNATURE);
        }
        
        int shutdown = header.getShutdown();
        if (!(shutdown == Pack.HARD_SHUTDOWN || shutdown == Pack.SOFT_SHUTDOWN))
        {
            throw new PackException(Pack.ERROR_SHUTDOWN);
        }
        
        if (shutdown == Pack.HARD_SHUTDOWN)
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
            throw new PackException(Pack.ERROR_IO_SIZE, e);
        }
        
        ByteBuffer reopen = ByteBuffer.allocateDirect(reopenSize);
        try
        {
            disk.read(fileChannel, reopen, header.getOpenBoundary());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_READ, e);
        }
        reopen.flip();
        
        SortedSet<Long> setOfAddressPages = new TreeSet<Long>();
        
        int addressPageCount = reopen.getInt();
        for (int i = 0; i < addressPageCount; i++)
        {
            setOfAddressPages.add(reopen.getLong());
        }
        
        Map<Long, ByteBuffer> mapOfTemporaryArrays = new HashMap<Long, ByteBuffer>();
        
        Pager pager = new Pager(file, fileChannel, disk, header,
                                mapOfStaticPages,
                                setOfAddressPages,
                                header.getDataBoundary(),
                                header.getOpenBoundary(),
                                mapOfTemporaryArrays);
        
        int blockPageCount = reopen.getInt();
        for (int i = 0; i < blockPageCount; i++)
        {
            long position = reopen.getLong();
            BlockPage blockPage = pager.getPage(position, UserPage.class, new UserPage());
            pager.returnUserPage(blockPage);
        }
        
        try
        {
            disk.truncate(fileChannel, header.getOpenBoundary());
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_TRUNCATE, e);
        }
        
        long openBoundary = header.getOpenBoundary();
        header.setShutdown(Pack.HARD_SHUTDOWN);
        header.setOpenBoundary(0L);

        try
        {
            header.write(disk, fileChannel);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_WRITE, e);
        }
        
        try
        {
            disk.force(fileChannel);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_FORCE, e);
        }

        Mutator mutator = pager.mutate();
        
        long temporaries = header.getTemporaries();
        do
        {
            ByteBuffer node = mutator.read(temporaries);
            mapOfTemporaryArrays.put(temporaries, node);
            long address = node.getLong(0);
            if (address != 0L)
            {
                setOfTemporaryBlocks.add(address);
            }
            temporaries = node.getLong(Pack.ADDRESS_SIZE);
        }
        while (temporaries != 0L);

        pager = new Pager(file, fileChannel, disk, header,
                          mapOfStaticPages,
                          setOfAddressPages,
                          header.getDataBoundary(),
                          openBoundary,
                          mapOfTemporaryArrays);
        
        return pager;
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
            throw new IllegalStateException();
        }
        ByteBuffer peek = rawPage.getByteBuffer();
        if (peek.getInt(Pack.CHECKSUM_SIZE) < 0)
        {
            return true;
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
            throw new PackException(Pack.ERROR_CORRUPT);
        }
        
        if (new AddressPage().verifyChecksum(rawPage, recovery))
        {
            AddressPage addresses = pager.getPage(position, AddressPage.class, new AddressPage());
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
            BlockPage test = new UserPage();
            test.verifyChecksum(rawPage, recovery);
            if (isBlockPage(rawPage, recovery))
            {
                break;
            }
            AddressPage addresses = new AddressPage();
            if (addresses.verifyChecksum(rawPage, recovery))
            {
                addresses = pager.getPage(position, AddressPage.class, addresses);
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
            position += recovery.getPager().getPageSize();
            pager.getUserBoundary().increment();
            pager.getInterimBoundary().increment();
        }

        for (;;)
        {
            pager.getInterimBoundary().increment();
            BlockPage blocks = new UserPage();
            if (blocks.verifyChecksum(rawPage, recovery))
            {
                blocks = pager.getPage(position, BlockPage.class, blocks);
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
            throw new PackException(Pack.ERROR_HEADER_CORRUPT);
        }
        ByteBuffer journals = ByteBuffer.allocate(header.getInternalJournalCount() * Pack.POSITION_SIZE);
        try
        {
            disk.read(fileChannel, journals, Pack.FILE_HEADER_SIZE);
        }
        catch (IOException e)
        {
            throw new PackException(Pack.ERROR_IO_READ, e);
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
            throw new PackException(Pack.ERROR_IO_SIZE, e);
        }

        Offsets offsets = new Offsets(header.getPageSize(), header.getInternalJournalCount(), header.getStaticPageSize());
        // First see if we can get to the data section cleanly.We're not
        // going to anything but checksum.
        
        Map<Long, ByteBuffer> mapOfTemporaryArrays = new HashMap<Long, ByteBuffer>();
        Pager pager = new Pager(file, fileChannel, disk, header,
                                new HashMap<URI, Long>(),
                                new TreeSet<Long>(),
                                offsets.getDataBoundary(),
                                offsets.getDataBoundary(),
                                mapOfTemporaryArrays);
        if (listOfAddressJournals.size() == 0 && listOfUserJournals.size() == 0)
        {
            return addressRecover(new Recovery(pager, fileSize, true));
        }

        return journalRecover(new Recovery(pager, fileSize, false));
    }
}