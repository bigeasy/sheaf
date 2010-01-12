package com.goodworkalan.sheaf;

import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import org.testng.annotations.Test;

public class SheafTest {
    @Test
    public void create() throws IOException {
        File file = File.createTempFile("sheaf", ".sheaf");
        file.deleteOnExit();
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        FileChannel fileChannel = raf.getChannel();

        Sheaf sheaf = new Sheaf(fileChannel, 1024, 24);
        
        assertEquals(sheaf.getOffset(), 24);
        assertEquals(sheaf.getPageSize(), 1024);
        
        Page page = sheaf.getPage(0L, Page.class, new Page());
        
        page.getRawPage().getByteBuffer().putInt(7);
        page.getRawPage().dirty(0, Integer.SIZE / Byte.SIZE);
        DirtyPageSet dirty = new DirtyPageSet();
        dirty.add(page.getRawPage());
        dirty.flush();
        
        sheaf = new Sheaf(fileChannel, 1024, 24);
        page = sheaf.getPage(0L, Page.class, new Page());
        assertEquals(page.getRawPage().getByteBuffer().getInt(), 7);
        
        fileChannel.close();
    }
}
