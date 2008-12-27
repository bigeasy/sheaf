package com.goodworkalan.pack;

import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;

final class BySizeTableIterator implements Iterator<Long>
{
    private Iterator<SortedSet<Long>> setsOfPages;
    
    private Iterator<Long> pages;
    
    public BySizeTableIterator(List<SortedSet<Long>> listOfSetsOfPages)
    {
        Iterator<SortedSet<Long>> setsOfPages = listOfSetsOfPages.iterator();
        Iterator<Long> pages = setsOfPages.next().iterator();
        while (!pages.hasNext() && setsOfPages.hasNext())
        {
            pages = setsOfPages.next().iterator();
        }
        this.setsOfPages = setsOfPages;
        this.pages = pages;
    }

    public boolean hasNext()
    {
        return pages.hasNext() || setsOfPages.hasNext();
    }
    
    public Long next()
    {
        while (!pages.hasNext())
        {
            if (!setsOfPages.hasNext())
            {
                throw new ArrayIndexOutOfBoundsException();
            }
            pages = setsOfPages.next().iterator();
        }
        long size = pages.next();
        while (!pages.hasNext() && setsOfPages.hasNext())
        {
            pages = setsOfPages.next().iterator();
        }
        return size;
    }
    
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}