package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

public abstract class AbstractRegion implements Region
{
    private final long position;
    
    private final ByteBuffer byteBuffer;
    
    private final Lock lock;
    
    public AbstractRegion(long position, ByteBuffer byteBuffer, Lock lock)
    {
        this.position = position;
        this.byteBuffer = byteBuffer;
        this.lock = lock;
    }
    
    protected abstract Dirtyable getDirtyable();
    
    public long getPosition()
    {
        return position;
    }

    public ByteBuffer getByteBuffer()
    {
        return byteBuffer;
    }

    public Lock getLock()
    {
        return lock;
    }
    
    public int getLength()
    {
        return getDirtyable().getLength();
    }
    
    public void dirty(int offset, int length)
    {
        getDirtyable().dirty(offset, length);
    }
    
    public void dirty()
    {
        getDirtyable().dirty();
    }
}
