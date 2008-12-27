package com.agtrz.pack;

import java.nio.ByteBuffer;

final class Pointer
{
    private final ByteBuffer slice;

    private final long position;

    private final Object mutex;

    public Pointer(ByteBuffer slice, long position, Object mutex)
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

    public Object getMutex()
    {
        return mutex;
    }
}