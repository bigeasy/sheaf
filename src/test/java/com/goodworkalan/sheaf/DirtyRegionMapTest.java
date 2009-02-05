package com.goodworkalan.sheaf;

import static junit.framework.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.testng.annotations.Test;

public class DirtyRegionMapTest
{
    private File newFile()
    {
        try
        {
            File file = File.createTempFile("momento", ".mto");
            file.deleteOnExit();
            return file;
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private void assertBuffer(FileChannel fileChannel,
            final ByteBuffer expected, ByteBuffer actual) throws IOException
    {
        expected.clear();
        actual.clear();
        fileChannel.read(actual, 0L);
        actual.flip();
        
        for (int i = 0; i < 64; i++)
        {
            try
            {
                assertEquals(expected.get(i), actual.get(i));
            }
            catch (IndexOutOfBoundsException e)
            {
                System.out.println(i);
                throw e;
            }
        }
    }

    @Test
    public void invalidate() throws IOException
    {
        FileChannel fileChannel = new RandomAccessFile(newFile(), "rw").getChannel();

        final ByteBuffer expected = ByteBuffer.allocateDirect(64);
        
        DirtyRegionMap regional = new DirtyRegionMap(0L)
        {
            @Override
            public ByteBuffer getByteBuffer()
            {
                return expected;
            }
        };
        
        for (int i = 0; i < 64; i++)
        {
            expected.put(i, (byte) 0);
        }
        
        regional.invalidate(0, 64);
        
        regional.write(fileChannel, 0);
        
        ByteBuffer actual = ByteBuffer.allocateDirect(64);
        assertBuffer(fileChannel, expected, actual);
        
        for (int i = 3; i < 6; i++)
        {
            expected.put(i, (byte) i);
        }
        regional.invalidate(3, 3);

        for (int i = 7; i < 10; i++)
        {
            expected.put(i, (byte) i);
        }
        regional.invalidate(7, 3);
        
        assertEquals(2, regional.regions.size());
        
        regional.write(fileChannel, 0);
        
        assertBuffer(fileChannel, expected, actual);
        
        for (int i = 3; i < 10; i++)
        {
            expected.put(i, (byte) -i);
        }
        regional.invalidate(3, 3);
        regional.invalidate(6, 4);

        assertEquals(1, regional.regions.size());

        regional.write(fileChannel, 0);
        
        assertBuffer(fileChannel, expected, actual);
        
        for (int i = 3; i < 10; i++)
        {
            expected.put(i, (byte) i);
        }
        regional.invalidate(6, 4);
        regional.invalidate(3, 3);
        assertEquals(1, regional.regions.size());

        regional.write(fileChannel, 0);
        assertBuffer(fileChannel, expected, actual);

        for (int i = 2; i < 11; i++)
        {
            expected.put(i, (byte) i);
        }
        // Two invalid regions.
        regional.invalidate(3, 3);
        regional.invalidate(7, 3);
        assertEquals(2, regional.regions.size());
        
        // First region extended by one.
        regional.invalidate(2, 4);
        assertEquals(2, regional.regions.size());
        
        // First region replace by larger region and merged into second region.
        regional.invalidate(2, 5);
        assertEquals(1, regional.regions.size());

        // Invalidating an already invalid region.
        regional.invalidate(3, 3);
        assertEquals(1, regional.regions.size());
        
        // Extending a region.
        regional.invalidate(8, 3);
        assertEquals(1, regional.regions.size());

        regional.write(fileChannel, 0);
        assertBuffer(fileChannel, expected, actual);
        
        for (int i = 2; i < 16; i++)
        {
            expected.put(i, (byte) -i);
        }
        // Swallowing entire regions.
        regional.invalidate(3, 3);
        regional.invalidate(7, 3);
        assertEquals(2, regional.regions.size());
        regional.invalidate(11, 2);
        assertEquals(3, regional.regions.size());
        regional.invalidate(2, 14);
        assertEquals(1, regional.regions.size());

        regional.write(fileChannel, 0);
        assertBuffer(fileChannel, expected, actual);
    }
}
