package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

public final class DirtyPageSet
{
    private final Sheaf pager;
    
    private final Checksum checksum;

    private final Map<Long, RawPage> mapOfPages;

    private final Map<Long, ByteBuffer> mapOfByteBuffers;

    private final int capacity;
    
    public DirtyPageSet(Sheaf pager, int capacity)
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
                throw new SheafException(101, e);
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
                    throw new SheafException(101, e);
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
            throw new SheafException(101, e);
        }
        try
        {
            disk.force(fileChannel);
        }
        catch (IOException e)
        {
            throw new SheafException(102, e);
        }
    }
    
    public void clear()
    {
        mapOfByteBuffers.clear();
        mapOfPages.clear();
    }
}