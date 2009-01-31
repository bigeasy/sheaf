package com.goodworkalan.sheaf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Opens pack files and performs recovery.
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
            return null;
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
            UserPage blockPage = pager.getPage(position, UserPage.class, new UserPage());
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
}