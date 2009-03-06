package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

// TODO Document.
public abstract class AbstractRegion implements Region
{
    // TODO Document.
    private final long position;
    
    // TODO Document.
    private final ByteBuffer byteBuffer;
    
    // TODO Document.
    private final Lock lock;
    
    // TODO Document.
    public AbstractRegion(long position, ByteBuffer byteBuffer, Lock lock)
    {
        this.position = position;
        this.byteBuffer = byteBuffer;
        this.lock = lock;
    }
    
    // TODO Document.
    protected abstract Dirtyable getDirtyable();
    
    // TODO Document.
    public long getPosition()
    {
        return position;
    }

    // TODO Document.
    public ByteBuffer getByteBuffer()
    {
        return byteBuffer;
    }

    // TODO Document.
    public Lock getLock()
    {
        return lock;
    }
    
    // TODO Document.
    public int getLength()
    {
        return getDirtyable().getLength();
    }
    
    // TODO Document.
    public void dirty(int offset, int length)
    {
        getDirtyable().dirty(offset, length);
    }
    
    // TODO Document.
    public void dirty()
    {
        getDirtyable().dirty();
    }
}
