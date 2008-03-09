/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import java.io.File;
import java.io.IOException;

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
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */