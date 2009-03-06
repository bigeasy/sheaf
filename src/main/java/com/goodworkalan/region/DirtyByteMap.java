package com.goodworkalan.region;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Dirty regions of a page as map of offsets to counts of dirty bytes. This
 * is a base class for raw pages and headers that want to write only the
 * regions of a page that have changed.
 * 
 * @author Alan Gutierrez
 */
public class DirtyByteMap implements Cleanable
{
    /** The map of dirty regions offsets to count of dirty bytes. */
    final SortedMap<Integer, Integer> dirtied;
    
    // TODO Document.
    private final int length;

    /**
     * Construct a dirty region map that will track the dirty regions of the
     * byte content at the given file position.
     * 
     * @param length
     *            The length of the region to track.
     */
    public DirtyByteMap(int length)
    {
        this.length = length;
        this.dirtied = new TreeMap<Integer, Integer>();
    }
    
    // TODO Document.
    public Map<Integer, Integer> toMap()
    {
        return Collections.unmodifiableMap(dirtied);
    }
    
    // TODO Document.
    public int getLength()
    {
        return length;
    }

    // TODO Document.
    public void dirty()
    {
        dirty(0, getLength());
    }

    /**
     * Mark as dirty the given length of bytes at the given offset.
     * <p>
     * If the specified region is overlaps or a another dirty region, the
     * regions are combined to create a single dirty region.
     * 
     * @param offset
     *            The offset of the dirty region.
     * @param length
     *            The length of the dirty region.
     */
    public void dirty(int offset, int length)
    {
        int start = offset;
        int end = offset + length;
        if (start < 0)
        {
            throw new IllegalStateException();
        }
        
        if (end > getLength())
        {
            throw new IllegalStateException();
        }
        
        INVALIDATE: for(;;)
        {
            Iterator<Map.Entry<Integer, Integer>> entries = dirtied.entrySet().iterator();
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
        dirtied.put(start, end);
    }
    
    // TODO Document.
    public void clean(int offset, int length)
    {
        int start = offset;
        int end = offset + length;
        if (start < 0)
        {
            throw new IllegalStateException();
        }
        
        if (end > getLength())
        {
            throw new IllegalStateException();
        }
        
        INVALIDATE: for(;;)
        {
            Iterator<Map.Entry<Integer, Integer>> entries = dirtied.entrySet().iterator();
            while (entries.hasNext())
            {
                Map.Entry<Integer, Integer> entry = entries.next();
                if (start <= entry.getKey() && end > entry.getKey())
                {
                    entries.remove();
                    if (end < entry.getValue())
                    {
                        dirtied.put(end, entry.getValue());
                    }
                    continue INVALIDATE;
                }
                else if (entry.getKey() < start && start < entry.getValue())
                {
                    entries.remove();
                    dirtied.put(entry.getKey(), start);
                    if (end < entry.getValue())
                    {
                        dirtied.put(end, entry.getValue());
                    }
                    continue INVALIDATE;
                }
                else if (entry.getValue() < start)
                {
                    break;
                }
            }
        }
    }
    
    // TODO Document.
    public void clean()
    {
        dirtied.clear();
    }

    // TODO Document.
    public void write(ByteBuffer bytes, FileChannel fileChannel, long position) throws IOException
    {
        for(Map.Entry<Integer, Integer> entry: toMap().entrySet())
        {
            bytes.limit(entry.getValue());
            bytes.position(entry.getKey());
            
            fileChannel.write(bytes, position + entry.getKey());
        }

        bytes.limit(bytes.capacity());
        
        clean();
    }
}