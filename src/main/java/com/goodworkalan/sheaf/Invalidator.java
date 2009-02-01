package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

// TODO This could be a separate utility class.
public abstract class Invalidator
{
    private long position;
    
    final SortedMap<Integer, Integer> regions;
    
    public Invalidator(long position)
    {
        this.position = position;
        this.regions = new TreeMap<Integer, Integer>();
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
            Iterator<Map.Entry<Integer, Integer>> entries = regions.entrySet().iterator();
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
        regions.put(start, end);
    }
    
    public void write(Disk disk, FileChannel fileChannel, int offset) throws IOException
    {
        ByteBuffer bytes = getByteBuffer();

        for(Map.Entry<Integer, Integer> entry: regions.entrySet())
        {
            bytes.limit(entry.getValue());
            bytes.position(entry.getKey());
            
            disk.write(fileChannel, bytes, offset + getPosition() + entry.getKey());
        }

        bytes.limit(bytes.capacity());
        
        regions.clear();
    }
}