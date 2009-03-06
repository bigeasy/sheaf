package com.goodworkalan.region;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.Lock;

import com.goodworkalan.sheaf.NullCleanable;

/**
 * <p>
 * When a region does not need to track which bytes are dirty, a
 * {@link NullCleanable} can be used to implement the <code>Dirtyable</code>
 * interface.
 * <p>
 * The lock can be ignored in the case of a region that is never accessed
 * concurrently. When a region is never accessed concurrently, locking the
 * uncontended lock ought not to create a significant performance penalty.
 * 
 * @author Alan Gutierrez
 */
public class BasicRegion extends AbstractRegion
{
    /** Used to track which bytes in the byte buffer are dirty. */
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
