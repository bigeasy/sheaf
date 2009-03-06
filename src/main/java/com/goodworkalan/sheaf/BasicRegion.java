package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

public class BasicRegion extends AbstractRegion
{
    public Dirtyable dirtyable;
    
    public BasicRegion(long position, ByteBuffer byteBuffer, Lock lock, Dirtyable dirtyable)
    {
        super(position, byteBuffer, lock);
        this.dirtyable = dirtyable;
    }
    
    @Override
    protected Dirtyable getDirtyable()
    {
        return dirtyable;
    }
}
