/* Copyright Alan Gutierrez 2006 */
package com.agtrz.pack;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

public class PackTestCase
extends TestCase
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

    public void testCreate()
    {
        new Pack.Creator().create(newFile()).close();
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */