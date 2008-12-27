package com.agtrz.pack;

import java.nio.ByteBuffer;

final class PositionSet
{
    private final boolean[] reserved;

    private final long position;

    public PositionSet(long position, int count)
    {
        this.position = position;
        this.reserved = new boolean[count];
    }

    public int getCapacity()
    {
        return reserved.length;
    }

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

    public synchronized void free(Pointer pointer)
    {
        int offset = (int) (pointer.getPosition() - position) / Pack.POSITION_SIZE;
        reserved[offset] = false;
        notify();
    }
}