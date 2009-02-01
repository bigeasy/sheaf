package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A structure referencing a position value stored at a specific position in the
 * file guarded by a mutex.
 * 
 * @author Alan Gutierrez
 */
public final class Region
{
    private final ByteBuffer slice;

    private final long position;

    private final Object mutex;

    public Region(ByteBuffer slice, long position, Object mutex)
    {
        this.slice = slice;
        this.position = position;
        this.mutex = mutex;
    }

    public long getPosition()
    {
        return position;
    }

    public ByteBuffer getByteBuffer()
    {
        return slice;
    }

    /**
     * Return the mutex used to guard the writing of the value position in the
     * file.
     * 
     * @return The mutex.
     */
    public Object getMutex()
    {
        return mutex;
    }
    
    public void write(Sheaf sheaf)
    {
        try
        {
            sheaf.getDisk().write(sheaf.getFileChannel(), slice, position);
        }
        catch (IOException e)
        {
            throw new SheafException(101, e);
        }
    }
}
