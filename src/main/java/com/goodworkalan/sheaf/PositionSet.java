package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;

/**
 * Maintains a set of allocated and free positions that reference a
 * position on range of the underlying file as a set of position. Used
 * by {@link Pager} to maintain a set of available slots in which to
 * write the first address of a chain of journal pages.
 *
 * @author Alan Gutierrez
 */
final class PositionSet
{
    /** Booleans indicating whether or not the page is free. */
    private final boolean[] reserved;

    /** The first position. */
    private final long position;

    /**
     * Create a position set begining at the offset position and
     * continuing for count positions. The size of this array in the
     * file is the size of a long by the count. Remember that the
     * positions store positions.
     *
     * @position The position offset of the first position value.
     * @count The number of position values kept at the position offset.
     */
    public PositionSet(long position, int count)
    {
        this.position = position;
        this.reserved = new boolean[count];
    }

    /**
     * Return the number of position values ket at the offset position.
     */
    public int getCapacity()
    {
        return reserved.length;
    }

    /**
     * Allocate an available position in which to store a position value
     * from the range covered by this position set waiting if the set is
     * empty. This method is synchronized so that the pointer set can be
     * drawn from by different mutators in different threads. If there
     * are no positions available, this method will wait until a
     * position is freed by another thread.
     *
     * @return A position in which to store a position value from the
     * range covered by this position set.
     */
    public synchronized Pointer allocate()
    {
        Pointer pointer = null;
        for (;;)
        {
            for (int i = 0; i < reserved.length && pointer == null; i++)
            {
                if (!reserved[i])
                {
                    reserved[i] = true;
                    pointer = new Pointer(ByteBuffer.allocateDirect(Pack.POSITION_SIZE), position + i * Pack.POSITION_SIZE, this);
                }
            }
            if (pointer == null)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException e)
                {
                }
            }
            else
            {
                break;
            }
        }
        return pointer;
    }

    /**
     * Free a position in the range covered by this position set
     * notifying any other threads waiting for a position that one is
     * available.
     *
     * @pointer A structure containing the position the free.
     */
    public synchronized void free(Pointer pointer)
    {
        int offset = (int) (pointer.getPosition() - position) / Pack.POSITION_SIZE;
        reserved[offset] = false;
        notify();
    }
}
