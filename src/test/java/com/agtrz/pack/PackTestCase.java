/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.Ignore;
import org.junit.Test;

public class PackTestCase
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

    @Test public void create()
    {
        new Pack.Creator().create(newFile()).close();
    }
    
    @Test public void getFile()
    {
        File file = newFile();
        Pack pack = new Pack.Creator().create(file);
        assertEquals(file, pack.getFile());
        pack.close();
    }

    private void assertBuffer(Pack.Disk disk, FileChannel fileChannel,
            final ByteBuffer expected, ByteBuffer actual) throws IOException
    {
        expected.clear();
        actual.clear();
        disk.read(fileChannel, actual, 0L);
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

    @Test(expected=java.lang.IllegalStateException.class) public void regionalLowerRange()
    {
        final ByteBuffer expected = ByteBuffer.allocateDirect(64);

        Pack.Regional regional = new Pack.Regional(0L)
        {
            @Override
            public ByteBuffer getByteBuffer()
            {
                return expected;
            }
        };
        regional.invalidate(-1, 10);
    }

    @Test(expected=java.lang.IllegalStateException.class) public void regionalUpperRange()
    {
        final ByteBuffer expected = ByteBuffer.allocateDirect(64);

        Pack.Regional regional = new Pack.Regional(0L)
        {
            @Override
            public ByteBuffer getByteBuffer()
            {
                return expected;
            }
        };
        regional.invalidate(0, 65);
    }

    @Test public void regional() throws IOException
    {
        Pack.Disk disk = new Pack.Disk();
        FileChannel fileChannel = disk.open(newFile());

        final ByteBuffer expected = ByteBuffer.allocateDirect(64);
        
        Pack.Regional regional = new Pack.Regional(0L)
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
        
        regional.write(disk, fileChannel);
        
        ByteBuffer actual = ByteBuffer.allocateDirect(64);
        assertBuffer(disk, fileChannel, expected, actual);
        
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
        
        assertEquals(2, regional.setOfRegions.size());
        
        regional.write(disk, fileChannel);
        
        assertBuffer(disk, fileChannel, expected, actual);
        
        for (int i = 3; i < 10; i++)
        {
            expected.put(i, (byte) -i);
        }
        regional.invalidate(3, 3);
        regional.invalidate(6, 4);

        assertEquals(1, regional.setOfRegions.size());

        regional.write(disk, fileChannel);
        
        assertBuffer(disk, fileChannel, expected, actual);
        
        for (int i = 3; i < 10; i++)
        {
            expected.put(i, (byte) i);
        }
        regional.invalidate(6, 4);
        regional.invalidate(3, 3);
        assertEquals(1, regional.setOfRegions.size());

        regional.write(disk, fileChannel);
        assertBuffer(disk, fileChannel, expected, actual);

        for (int i = 2; i < 11; i++)
        {
            expected.put(i, (byte) i);
        }
        // Two invalid regions.
        regional.invalidate(3, 3);
        regional.invalidate(7, 3);
        assertEquals(2, regional.setOfRegions.size());
        
        // First region extended by one.
        regional.invalidate(2, 4);
        assertEquals(2, regional.setOfRegions.size());
        
        // First region replace by larger region and merged into second region.
        regional.invalidate(2, 5);
        assertEquals(1, regional.setOfRegions.size());

        // Invalidating an already invalid region.
        regional.invalidate(3, 3);
        assertEquals(1, regional.setOfRegions.size());
        
        // Extending a region.
        regional.invalidate(8, 3);
        assertEquals(1, regional.setOfRegions.size());

        regional.write(disk, fileChannel);
        assertBuffer(disk, fileChannel, expected, actual);
        
        for (int i = 2; i < 16; i++)
        {
            expected.put(i, (byte) -i);
        }
        // Swallowing entire regions.
        regional.invalidate(3, 3);
        regional.invalidate(7, 3);
        assertEquals(2, regional.setOfRegions.size());
        regional.invalidate(11, 2);
        assertEquals(3, regional.setOfRegions.size());
        regional.invalidate(2, 14);
        assertEquals(1, regional.setOfRegions.size());

        regional.write(disk, fileChannel);
        assertBuffer(disk, fileChannel, expected, actual);
    }

    @Test public void reopen()
    {
        File file = newFile();
        new Pack.Creator().create(file).close();
        new Pack.Opener().open(file).close();
        new Pack.Opener().open(file).close();
    }

    @Test public void commit()
    {
        File file = newFile();
        Pack pack = new Pack.Creator().create(file);
        Pack.Mutator mutator = pack.mutate();
        mutator.commit();
        pack.close();
        new Pack.Opener().open(file).close();
    }

    @Test public void allocate()
    {
        File file = newFile();
        Pack pack = new Pack.Creator().create(file);
        Pack.Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        pack.close();
        new Pack.Opener().open(file).close();
    }
    
    @Test public void fileNotFoundOpen()
    {
        File file = new File("/not/very/likely/harpsicord");
        try
        {
            new Pack.Opener().open(file);
        }
        catch (Pack.Danger e)
        {
            assertEquals(Pack.ERROR_FILE_NOT_FOUND, e.getCode());
            return;
        }
        fail("Expected exception not thrown."); 
    }
    
    @Test public void fileNotFoundCreate()
    {
        File file = new File("/not/very/likely/harpsicord");
        try
        {
            new Pack.Creator().create(file);
        }
        catch (Pack.Danger e)
        {
            assertEquals(Pack.ERROR_FILE_NOT_FOUND, e.getCode());
            return;
        }
        fail("Expected exception not thrown."); 
    }
    
    @Test public void badSignature() throws IOException
    {
        Pack.Disk disk = new Pack.Disk();
        File file = newFile();
        
        new Pack.Creator().create(file).close();
        
        ByteBuffer bytes = ByteBuffer.allocateDirect(1);
        bytes.put((byte) 0);
        bytes.flip();

        FileChannel fileChannel = disk.open(file);
        fileChannel.write(bytes, 0L);
        fileChannel.close();

        try
        {
            new Pack.Opener().open(file);
        }
        catch (Pack.Danger e)
        {
            assertEquals(Pack.ERROR_SIGNATURE, e.getCode());
            return;
        }

        fail("Expected exception not thrown.");
    }

    @Test public void relocatable()
    {
        File file = newFile();
        Pack pack = new Pack.Creator().create(file);
        Pack.Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        pack.close();
        
        pack = new Pack.Opener().open(file);
        Pack.Pager pager = pack.pager;
        Pack.Page page = pager.getPage(8192, new Pack.RelocatablePage());
        page = pager.getPage(8192, new Pack.BlockPage(false));
        assertEquals(8192, page.getRawPage().getPosition());
    }

    // 17% 10%

    @Test public void write()
    {
        File file = newFile();
        Pack pack = new Pack.Creator().create(file);
        Pack.Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put(i, (byte) i);
        }
        mutator.write(address, bytes);

        bytes.clear();
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }
        
        mutator.commit();
        
        mutator = pack.mutate();

        bytes.clear();
        mutator.read(address, bytes);
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }

    @Ignore @Test public void recover()
    {
        Pack.Creator newPack = new Pack.Creator();
        newPack.setDisk(new Pack.Disk()
        {
            int count = 0;
            
            public int write(FileChannel fileChannel, ByteBuffer dst, long position) throws IOException
            {
                if (position == 0)
                {
                    if (count++ == 1)
                    {
                        fileChannel.close();
                        throw new IOException();
                    }
                }
                
                return fileChannel.write(dst, position);
            }
        });
        File file = newFile();
        Pack pack = newPack.create(file);
        Pack.Mutator mutator = pack.mutate();
        long address = mutator.allocate(20);
        ByteBuffer bytes = ByteBuffer.allocateDirect(20);
        for (int i = 0; i < 20; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        mutator.write(address, bytes);
        try
        {
            mutator.commit();
        }
        catch (Error e)
        {
        }
        Pack.Opener opener = new Pack.Opener();
        opener.open(file);
        
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */