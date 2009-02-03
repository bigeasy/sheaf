package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A structure referencing a position value stored at a specific position in the
 * file guarded by a mutex.
 * <p>
 * TODO Maybe move back to pack. You're not actually using this here.
 * 
 * @author Alan Gutierrez
 */
public final class Segment
{
    private final ByteBuffer byteBuffer;

    private final long position;

    private final Object mutex;

    public Segment(ByteBuffer byteBuffer, long position, Object mutex)
    {
        this.byteBuffer = byteBuffer;
        this.position = position;
        this.mutex = mutex;
    }

    /**
     * Return the position of the segment.
     * 
     * @return The position of the segment.
     */
    public long getPosition()
    {
        return position;
    }

    /**
     * Return the byte buffer containing the bytes in the segment.
     * 
     * @return The byte buffer.
     */
    public ByteBuffer getByteBuffer()
    {
        return byteBuffer;
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
            sheaf.getDisk().write(sheaf.getFileChannel(), byteBuffer, position);
        }
        catch (IOException e)
        {
            throw new SheafException(101, e);
        }
    }
}
