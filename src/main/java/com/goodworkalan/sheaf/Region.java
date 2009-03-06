package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

public interface Region extends Dirtyable
{
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
