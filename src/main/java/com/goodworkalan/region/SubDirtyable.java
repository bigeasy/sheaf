package com.goodworkalan.region;

// TODO Document.
public class SubDirtyable implements Cleanable
{
    // TODO Document.
    private final int offset;
    
    // TODO Document.
    private final int length;
    
    // TODO Document.
    private final Cleanable dirtyable;

    // TODO Document.
    public SubDirtyable(Cleanable dirtyable, int offset, int length)
    {
        this.offset = offset;
        this.length = length;
        this.dirtyable = dirtyable;
    }
    
    // TODO Document.
    public int getLength()
    {
        return length;
    }

    // TODO Document.
    public void dirty()
    {
        dirty(0, length);
    }

    // TODO Document.
    public void dirty(int offset, int length)
    {
        if (length > getLength())
        {
            throw new IndexOutOfBoundsException();
        }
        dirtyable.dirty(this.offset + offset, length);
    }
    
    // TODO Document.
    public void clean(int offset, int length)
    {
        if (length > getLength())
        {
            throw new IndexOutOfBoundsException();
        }
        dirtyable.clean(this.offset + offset, length);
    }
    
    // TODO Document.
    public void clean()
    {
        clean(0, length);
    }
}
