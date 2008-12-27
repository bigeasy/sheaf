/**
 * 
 */
package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;


final class BySizeTable implements Iterable<Long>
{
    private final int alignment;
    
    private final Map<Long, Integer> mapOfPageToSize;

    private final List<SortedSet<Long>> listOfSetsOfPages;
    
    private final SortedMap<Long, Integer> mapToIgnore;

    public BySizeTable(int pageSize, int alignment)
    {
        assert pageSize % alignment == 0;

        ArrayList<SortedSet<Long>> listOfSetsOfPages = new ArrayList<SortedSet<Long>>(pageSize / alignment);

        for (int i = 0; i < pageSize / alignment; i++)
        {
            listOfSetsOfPages.add(new TreeSet<Long>());
        }

        this.alignment = alignment;
        this.listOfSetsOfPages = listOfSetsOfPages;
        this.mapToIgnore = new TreeMap<Long, Integer>();
        this.mapOfPageToSize = new HashMap<Long, Integer>();
    }
    
    public int getSize()
    {
        return mapOfPageToSize.size();
    }
    
    public boolean contains(long position)
    {
        return mapOfPageToSize.containsKey(position);
    }
    
    public Iterator<Long> iterator()
    {
        return mapOfPageToSize.keySet().iterator();
    }

    public synchronized void add(BlockPage blocks)
    {
        add(blocks.getRawPage().getPosition(), blocks.getRemaining());
    }
    
    public synchronized void add(long position, int remaining)
    {
        if (!mapToIgnore.containsKey(position))
        {
            // Maybe don't round down if exact.
            int aligned = remaining / alignment * alignment;
            if (aligned != 0)
            {
                mapOfPageToSize.put(position, remaining);
                listOfSetsOfPages.get(aligned / alignment).add(position);
            }
        }
    }
    
    public int remove(long position)
    {
        Integer remaining = mapOfPageToSize.get(position);
        if (remaining != null)
        {
            mapOfPageToSize.remove(position);
            int aligned = remaining / alignment * alignment;
            Set<Long> listOfPositions = listOfSetsOfPages.get(aligned / alignment);
            listOfPositions.remove(position);
            return remaining;
        }
        return 0;
    }

    public synchronized boolean reserve(long position)
    {
        remove(position);
        Integer count = mapToIgnore.get(position);
        mapToIgnore.put(position, count == null ? 1 : count + 1);
        return false;
    }
    
    public synchronized void release(long position)
    {
        Integer count = mapToIgnore.remove(position);
        if (count != null && count != 1)
        {
            mapToIgnore.put(position, count - 1);
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
            for (int i = aligned / alignment; bestFit == 0L && i < listOfSetsOfPages.size(); i++)
            {
                if (!listOfSetsOfPages.get(i).isEmpty())
                {
                    bestFit = listOfSetsOfPages.get(i).first();
                    listOfSetsOfPages.get(i).remove(bestFit);
                    mapOfPageToSize.remove(bestFit);
                }
            }
        }
        return bestFit;
    }

    // FIXME We can compact even further by joining vacuumed pages.
    public synchronized void join(BySizeTable pagesBySize, Set<Long> setOfDataPages, Map<Long, Movable> mapOfPages, MoveNode moveNode)
    {
        int pageSize = listOfSetsOfPages.size() * alignment;
        for (Map.Entry<Long, Integer> entry : pagesBySize.mapOfPageToSize.entrySet())
        {
            int needed = pageSize - (Pack.BLOCK_PAGE_HEADER_SIZE + entry.getValue());
            long found = bestFit(needed);
            if (found != 0L)
            {
                setOfDataPages.add(found);
                mapOfPages.put(entry.getKey(), new Movable(moveNode, found, 0));
            }
        }
    }
    
    public void clear()
    {
        for (SortedSet<Long> setOfPages : listOfSetsOfPages)
        {
            setOfPages.clear();
        }
        mapOfPageToSize.clear();
        mapToIgnore.clear();
    }
}