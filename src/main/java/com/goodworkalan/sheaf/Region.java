package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

/**
 * A lockable and dirtyable region of a file channel. A region maps a byte
 * buffer to a lock to guard writing to the byte buffer and a position in the
 * file. A region implements the {@link Dirtyable} interface to that is can
 * track which bytes in the byte buffer need to be written to disk.
 * 
 * @author Alan Gutierrez
 */
public interface Region extends Dirtyable
{
    /**
     * Get the lock used to guard the underlying byte buffer.
     * 
     * @return The lock used to guard the region.
     */
    public Lock getLock();

    /**
     * Get the file position at the beginning of the region.
     * 
     * @return The file position at the beginning of the region.
     */
    public long getPosition();
    
    /**
     * Return the byte content associated with this dirty region map. Subclasses
     * will define this method to return a byte buffer that corresponds to the
     * given file position, accounting for any offsets.
     * 
     * @return The byte content associated with this dirty region map.
     */
    public ByteBuffer getByteBuffer();
}
