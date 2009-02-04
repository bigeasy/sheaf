package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

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
    /** The bytes at the file position of the segment. */
    private final ByteBuffer byteBuffer;

    /** The file position of the segment. */
    private final long position;

    /** A mutex to hold when writing to this segment. */
    private final Object mutex;

    /**
     * Create a segment that writes the given byte buffer to the given position.
     * The mutex is used with a synchronized block to protect writing the
     * segment.
     * 
     * @param byteBuffer
     *            The byte content of the segment.
     * @param position
     *            The file position of the segment.
     * @param mutex
     *            A mutex to protect writing to the segment.
     */
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
     * <p>
     * TODO Not actually in use.
     *
     * @return The mutex.
     */
    public Object getMutex()
    {
        return mutex;
    }
    
    /**
     * Write the byte content to the file position in the given file channel.
     * 
     * @param fileChannel The file channel.
     */
    public void write(FileChannel fileChannel)
    {
        try
        {
            fileChannel.write(byteBuffer, position);
        }
        catch (IOException e)
        {
            throw new SheafException(101, e);
        }
    }
}
