package com.goodworkalan.sheaf;

public class NullCleanable implements Cleanable
{
    private final int length;
    
    public NullCleanable(int length)
    {
        this.length = length;
    }

    public void clean(int offset, int length)
    {
    }

    public void clean()
    {
    }

    public void dirty()
    {
    }

    public void dirty(int offset, int length)
    {
    }

    public int getLength()
    {
        return length;
    }
}
