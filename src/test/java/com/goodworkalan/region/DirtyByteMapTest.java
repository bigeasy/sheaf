package com.goodworkalan.region;

import static junit.framework.Assert.assertEquals;

import java.io.IOException;

import org.testng.annotations.Test;

import com.goodworkalan.region.DirtyByteMap;

public class DirtyByteMapTest
{
    @Test
    public void dirty() throws IOException
    {
        DirtyByteMap dirtyByteMap = new DirtyByteMap(64);
        
        dirtyByteMap.dirty(0, 64);
        
        dirtyByteMap.dirty(3, 3);

        dirtyByteMap.dirty(7, 3);
        
        dirtyByteMap.dirty(3, 3);
        dirtyByteMap.dirty(6, 4);
        
        dirtyByteMap.dirty(6, 4);
        dirtyByteMap.dirty(3, 3);
        assertEquals(1, dirtyByteMap.dirtied.size());

        // Two invalid regions.
        dirtyByteMap.dirty(3, 3);
        dirtyByteMap.dirty(7, 3);
        assertEquals(2, dirtyByteMap.dirtied.size());
        
        // First region extended by one.
        dirtyByteMap.dirty(2, 4);
        assertEquals(2, dirtyByteMap.dirtied.size());
        
        // First region replace by larger region and merged into second region.
        dirtyByteMap.dirty(2, 5);
        assertEquals(1, dirtyByteMap.dirtied.size());

        // Invalidating an already invalid region.
        dirtyByteMap.dirty(3, 3);
        assertEquals(1, dirtyByteMap.dirtied.size());
        
        // Extending a region.
        dirtyByteMap.dirty(8, 3);
        assertEquals(1, dirtyByteMap.dirtied.size());
        
        // Swallowing entire regions.
        dirtyByteMap.dirty(3, 3);
        dirtyByteMap.dirty(7, 3);
        assertEquals(2, dirtyByteMap.dirtied.size());
        dirtyByteMap.dirty(11, 2);
        assertEquals(3, dirtyByteMap.dirtied.size());
        dirtyByteMap.dirty(2, 14);
        assertEquals(1, dirtyByteMap.dirtied.size());
    }

    @Test(expectedExceptions=java.lang.IllegalStateException.class)
    public void regionalLowerRange()
    {
        DirtyByteMap dirtyByteMap = new DirtyByteMap(64);
        dirtyByteMap.dirty(-1, 10);
    }

    @Test(expectedExceptions=java.lang.IllegalStateException.class)
    public void regionalUpperRange()
    {
        DirtyByteMap dirtyByteMap = new DirtyByteMap(64);
        dirtyByteMap.dirty(0, 65);
    }
}

