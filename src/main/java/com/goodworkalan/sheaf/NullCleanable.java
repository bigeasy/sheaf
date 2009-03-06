package com.goodworkalan.sheaf;

// TODO Document.
public class NullCleanable implements Cleanable
{
    // TODO Document.
    private final int length;
    
    // TODO Document.
    public NullCleanable(int length)
    {
        this.length = length;
    }

    // TODO Document.
    public void clean(int offset, int length)
    {
    }

    // TODO Document.
    public void clean()
    {
    }

    // TODO Document.
    public void dirty()
    {
    }

    // TODO Document.
    public void dirty(int offset, int length)
    {
    }

    // TODO Document.
    public int getLength()
    {
        return length;
    }
}
