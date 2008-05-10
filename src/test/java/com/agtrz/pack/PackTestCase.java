/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

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
    
    @Ignore @Test public void reopen()
    {
        File file = newFile();
        new Pack.Creator().create(file).close();
        new Pack.Opener().open(file).close();
    }

    @Ignore @Test public void recover()
    {
        Pack.Creator newPack = new Pack.Creator();
        newPack.setWriteListener(new Pack.WriteListener()
        {
            public void write(boolean commit, FileChannel fileChannel, long position, ByteBuffer bytes) throws IOException
            {
                if (commit && position == 0)
                {
                    fileChannel.close();
                    throw new IOException();
                }
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