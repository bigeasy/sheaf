package com.goodworkalan.sheaf;

public class SubDirtyable implements Cleanable
{
    private final int offset;
    
    private final int length;
    
    private final Cleanable dirtyable;

    public SubDirtyable(Cleanable dirtyable, int offset, int length)
    {
        this.offset = offset;
        this.length = length;
        this.dirtyable = dirtyable;
    }
    
    public int getLength()
    {
        return length;
    }

    public void dirty()
    {
        dirty(0, length);
    }

    public void dirty(int offset, int length)
    {
        if (length > getLength())
        {
            throw new IndexOutOfBoundsException();
        }
        dirtyable.dirty(this.offset + offset, length);
    }
    
    public void clean(int offset, int length)
    {
        if (length > getLength())
        {
            throw new IndexOutOfBoundsException();
        }
        dirtyable.clean(this.offset + offset, length);
    }
    
    public void clean()
    {
        clean(0, length);
    }
}
