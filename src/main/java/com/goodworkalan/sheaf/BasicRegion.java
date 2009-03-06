package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

// TODO Document.
public class BasicRegion extends AbstractRegion
{
    // TODO Document.
    public Dirtyable dirtyable;
    
    // TODO Document.
    public BasicRegion(long position, ByteBuffer byteBuffer, Lock lock, Dirtyable dirtyable)
    {
        super(position, byteBuffer, lock);
        this.dirtyable = dirtyable;
    }
    
    // TODO Document.
    @Override
    protected Dirtyable getDirtyable()
    {
        return dirtyable;
    }
}
