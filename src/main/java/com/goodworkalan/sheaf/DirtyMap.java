package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
public abstract class DirtyMap
{
    /** The file position at the beginning of the region.*/
    private long position;
    
    /** The map of dirty regions offsets to count of dirty bytes. */
    final SortedMap<Integer, Integer> regions;

    /**
     * Construct a dirty region map that will track the dirty regions of the
     * byte content at the given file position.
     * 
     * @param position
     *            The position of byte content.
     */
    public DirtyMap(long position)
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
    protected synchronized void setPosition(long position)
    {
        this.position = position;
    }

    /**
     * Return the byte content associated with this dirty region map. Subclasses
     * will define this method to return a byte buffer that corresponds to the
     * given file position, accounting for any offsets.
     * 
     * @return The byte content associated with this dirty region map.
     */
    public abstract ByteBuffer getByteBuffer();

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

    /**
     * Write the dirty regions to the given file channel using the given disk at
     * the position of this dirty region map offset by the given offset.
     * 
     * @param fileChannel
     *            The file channel to write to.
     * @param offset
     *            An offset to add to the dirty region map file position.
     * @throws IOException
     *             If an I/O error occours.
     */
    public void write(FileChannel fileChannel, int offset) throws IOException
    {
        ByteBuffer bytes = getByteBuffer();

        for(Map.Entry<Integer, Integer> entry: regions.entrySet())
        {
            bytes.limit(entry.getValue());
            bytes.position(entry.getKey());
            
            fileChannel.write(bytes, offset + getPosition() + entry.getKey());
        }

        bytes.limit(bytes.capacity());
        
        regions.clear();
    }
}