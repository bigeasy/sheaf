/* Copyright Alan Gutierrez 2006 */
package com.goodworkalan.pack;

import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;

// TODO Move to pack-io.
public class ByteBufferOutputStream
extends OutputStream
{
    private final ByteBuffer bytes;
    
    public ByteBufferOutputStream(ByteBuffer bytes)
    {
        this.bytes = bytes;
    }

    @Override
    public void write(int b) throws IOException
    {
        try
        {
            bytes.put((byte) b);
        }
        catch (BufferOverflowException e)
        {
            throw new EOFException("Buffer overflow."); 
        }
    }
    
    @Override
    public void write(byte[] b, int off, int len) throws IOException
    {
        try
        {
            bytes.put(b, off, len);
        }
        catch (BufferOverflowException e)
        {
            throw new EOFException("Buffer overflow."); 
        }
    }
}

/* vim: set et sw=4 ts=4 ai tw=78 nowrap: */