/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.pack;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

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
        new Creator().create(newFile()).close();
    }
    
    @Test public void getFile()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        assertEquals(file, pack.getFile());
        pack.close();
    }

    private void assertBuffer(Disk disk, FileChannel fileChannel,
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

        Regional regional = new Regional(0L)
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

        Regional regional = new Regional(0L)
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
        Disk disk = new Disk();
        FileChannel fileChannel = disk.open(newFile());

        final ByteBuffer expected = ByteBuffer.allocateDirect(64);
        
        Regional regional = new Regional(0L)
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
        new Creator().create(file).close();
        new Opener().open(file).close();
        new Opener().open(file).close();
    }

    @Test public void commit()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.commit();
        pack.close();
        new Opener().open(file).close();
    }

    @Test public void allocate()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        pack.close();
        new Opener().open(file).close();
    }
    
    @Test public void fileNotFoundOpen()
    {
        File file = new File("/not/very/likely/harpsicord");
        try
        {
            new Opener().open(file);
        }
        catch (PackException e)
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
            new Creator().create(file);
        }
        catch (PackException e)
        {
            assertEquals(Pack.ERROR_FILE_NOT_FOUND, e.getCode());
            return;
        }
        fail("Expected exception not thrown."); 
    }
    
    @Test public void badSignature() throws IOException
    {
        Disk disk = new Disk();
        File file = newFile();
        
        new Creator().create(file).close();
        
        ByteBuffer bytes = ByteBuffer.allocateDirect(1);
        bytes.put((byte) 0);
        bytes.flip();

        FileChannel fileChannel = disk.open(file);
        fileChannel.write(bytes, 0L);
        fileChannel.close();

        try
        {
            new Opener().open(file);
        }
        catch (PackException e)
        {
            assertEquals(Pack.ERROR_SIGNATURE, e.getCode());
            return;
        }

        fail("Expected exception not thrown.");
    }

    @Test public void relocatable()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        pack.close();
        
        Pager pager = (Pager) new Opener().open(file);
        Page page = pager.getPage(8192, RelocatablePage.class, new RelocatablePage());
        page = pager.getPage(8192, UserPage.class, new UserPage());
        assertEquals(8192, page.getRawPage().getPosition());
    }

    private ByteBuffer get64bytes()
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        return bytes;
    }
    
    @Test public void write()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        mutator.write(address, bytes);

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }
        
        mutator.commit();
        
        mutator = pack.mutate();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }
    
    @Test public void rewrite()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        mutator = pack.mutate();
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        
        mutator.write(address, bytes);
        
        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        mutator = pack.mutate();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();

        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }
    
    @Test public void collect()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        System.gc();
        System.gc();
        System.gc();
        
        mutator = pack.mutate();
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        
        mutator.write(address, bytes);
        
        System.gc();
        System.gc();
        System.gc();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        System.gc();
        System.gc();
        System.gc();

        mutator = pack.mutate();

        bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();

        for (int i = 0; i < 64; i++)
        {
            assertEquals((byte) i, bytes.get());
        }

        mutator.commit();
        
        pack.close();
    }
    
    @Test public void rewriteMany()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);

        rewrite(pack, 12);
                
        pack.close();
    }

    public void rewrite(Pack pack, int count)
    {
        Mutator mutator = pack.mutate();
        long[] addresses = new long[count];
        for (int i = 0; i < count; i++)
        {
            addresses[i] = mutator.allocate(64);
        }
        mutator.commit();
        
        mutator = pack.mutate();
        for (int i = 0; i < count; i++)
        {
            ByteBuffer bytes = ByteBuffer.allocateDirect(64);
            for (int j = 0; j < 64; j++)
            {
                bytes.put((byte) j);
            }
            bytes.flip();
            
            mutator.write(addresses[i], bytes);
        }

        for (int i = 0; i < count; i++)
        {
            assertBuffer(mutator, addresses[i], 64);
        }

        mutator.commit();
        mutator = pack.mutate();
        for (int i = 0; i < count; i++)
        {
            assertBuffer(mutator, addresses[i], 64);
        }

        mutator.commit();
    }

    private void assertBuffer(Mutator mutator, long address, int count)
    {
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        mutator.read(address, bytes);
        bytes.flip();
        
        for (int i = 0; i < count; i++)
        {
            assertEquals((byte) i, bytes.get());
        }
    }

    @Test public void free()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();

        boolean thrown = false;
        mutator = pack.mutate();
        try
        {
            mutator.read(address, ByteBuffer.allocateDirect(64));
        }
        catch (PackException e)
        {
            thrown = true;
            assertEquals(Pack.ERROR_READ_FREE_ADDRESS, e.getCode());
        }
        assertTrue(thrown);
        mutator.commit();

        pack.close();
    }

    @Test public void freeAndClose()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        pack.close();
        pack = new Opener().open(file);
        
        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();

        boolean thrown = false;
        mutator = pack.mutate();
        try
        {
            mutator.read(address, ByteBuffer.allocateDirect(64));
        }
        catch (PackException e)
        {
            thrown = true;
            assertEquals(Pack.ERROR_READ_FREE_ADDRESS, e.getCode());
        }
        assertTrue(thrown);
        mutator.commit();

        pack.close();
    }
    
    @Test public void freeWithContext()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.commit();
        
        rewrite(pack, 3);

        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();

        boolean thrown = false;
        mutator = pack.mutate();
        try
        {
            mutator.read(address, ByteBuffer.allocateDirect(64));
        }
        catch (PackException e)
        {
            thrown = true;
            assertEquals(Pack.ERROR_READ_FREE_ADDRESS, e.getCode());
        }
        assertTrue(thrown);
        mutator.commit();

        pack.close();
    }

    @Test public void mulipleJournalPages()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);

        rewrite(pack, 1);
        
        Mutator mutator = pack.mutate();
        for (int i = 0; i < 800; i++)
        {
            mutator.allocate(64);
        }
        mutator.commit();
        
        pack.close();
    }

    @Test public void moveUserPageForAddress()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);

        rewrite(pack, 1);
        
        Mutator mutator = pack.mutate();
        for (int i = 0; i < 1000; i++)
        {
            mutator.allocate(64);
        }
        mutator.commit();
        
        pack.close();
    }
    
    @Test(expected=java.lang.UnsupportedOperationException.class)
    public void bySizeTableIteratorRemove()
    {
        List<SortedSet<Long>> listOfListsOfSizes = new ArrayList<SortedSet<Long>>();
        listOfListsOfSizes.add(new TreeSet<Long>());
        Iterator<Long> iterator = new BySizeTableIterator(listOfListsOfSizes);
        iterator.remove();
    }
    
    @Test(expected=java.lang.ArrayIndexOutOfBoundsException.class)
    public void bySizeTableIteratorOutOfBounds()
    {
        List<SortedSet<Long>> listOfListsOfSizes = new ArrayList<SortedSet<Long>>();
        listOfListsOfSizes.add(new TreeSet<Long>());
        Iterator<Long> iterator = new BySizeTableIterator(listOfListsOfSizes);
        iterator.next();
    }
    
    @Test public void staticPages()
    {
        Creator creator = new Creator();
        creator.addStaticPage(URI.create("http://one.com/"), 64);
        creator.addStaticPage(URI.create("http://two.com/"), 64);
        File file = newFile();
        Pack pack = creator.create(file);
        Mutator mutator = pack.mutate();
        mutator.write(mutator.getPack().getStaticPages().get(URI.create("http://one.com/")), get64bytes());
        mutator.commit();
    }
    
    @Ignore @Test public void moveInterimPageForAddress()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);

        rewrite(pack, 8000);
                
        pack.close();
    }

    @Test public void softRecover()
    {
        Creator newPack = new Creator();
        newPack.setDisk(new Disk()
        {
            int count = 0;

            @Override
            public FileChannel truncate(FileChannel fileChannel, long size) throws IOException
            {
                if (count++ == 2)
                {
                    fileChannel.close();
                    throw new IOException();
                }
                return fileChannel.truncate(size);
            }
//            Saving this for a different sort of recover.
//            public int write(FileChannel fileChannel, ByteBuffer dst, long position) throws IOException
//            {
//                if (position == 52)
//                {
//                    if (count++ == 1)
//                    {
//                        fileChannel.close();
//                        throw new IOException();
//                    }
//                }
//                
//                return fileChannel.write(dst, position);
//            }
        });
        File file = newFile();
        Pack pack = newPack.create(file);
        Mutator mutator = pack.mutate();
        long address = mutator.allocate(64);
        ByteBuffer bytes = ByteBuffer.allocateDirect(64);
        for (int i = 0; i < 64; i++)
        {
            bytes.put((byte) i);
        }
        bytes.flip();
        mutator.write(address, bytes);
        mutator.commit();
        boolean thrown = false;
        try
        {
            pack.close();
        }
        catch (PackException e)
        {
            thrown = true;
        }
        assertTrue(thrown);
        Opener opener = new Opener();
        pack = opener.open(file);
        mutator = pack.mutate();
        assertBuffer(mutator, address, 64);
        mutator.commit();
        pack.close();
    }
    
    @Test public void rollback() 
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        mutator.commit();
        mutator = pack.mutate();
        long address = mutator.allocate(64);
        mutator.rollback();
        pack.close();
        new Opener().open(file).close();
        pack = new Opener().open(file);
        mutator = pack.mutate();
        assertEquals(address, mutator.allocate(64));
        mutator.rollback();
        pack.close();
    }
    
    @Test public void vacuum()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        long address = mutator.allocate(64);
        mutator.commit();
        rewrite(pack, 4);
        pack.close();
        pack = new Opener().open(file);
        mutator = pack.mutate();
        mutator.free(address);
        mutator.commit();
        mutator = pack.mutate();
        assertEquals(address, mutator.allocate(64));
        mutator.commit();
        pack.close();
    }
    
    @Test public void vacuumAtOffset()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(64);
        long address1 = mutator.allocate(64);
        mutator.allocate(64);
        long address2 = mutator.allocate(64);
        mutator.commit();
        rewrite(pack, 4);
        pack.close();
        pack = new Opener().open(file);
        mutator = pack.mutate();
        mutator.free(address1);
        mutator.free(address2);
        mutator.commit();
        mutator = pack.mutate();
        assertEquals(address1, mutator.allocate(64));
        mutator.commit();
        pack.close();
    }

    @Test public void temporary()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.temporary(64);
        mutator.commit();
        pack.close();
        Opener opener = new Opener();
        pack = opener.open(file);
        mutator = pack.mutate();
        for (long address : opener.getTemporaryBlocks())
        {
            mutator.free(address);
        }
        mutator.commit();
        pack.close();
        opener = new Opener();
        pack = opener.open(file);
        assertEquals(0, opener.getTemporaryBlocks().size());
    }
    
    
    @Test public void unallocate()
    {
        File file = newFile();
        Pack pack = new Creator().create(file);
        Mutator mutator = pack.mutate();
        mutator.allocate(13);
        long allocate = mutator.allocate(9);
        long write1 = mutator.allocate(72);
        long write2 = mutator.allocate(82);
        mutator.free(allocate);
        mutator.commit();
        mutator.write(write1, get64bytes());
        mutator.write(write2, get64bytes());
        mutator.free(write2);
        mutator.commit();
        mutator.allocate(21);
        allocate = mutator.allocate(37);
        mutator.free(allocate);
        mutator.allocate(73);
        mutator.commit();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */