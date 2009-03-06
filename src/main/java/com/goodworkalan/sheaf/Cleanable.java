package com.goodworkalan.sheaf;

public interface Cleanable extends Dirtyable
{
    public void clean(int offset, int length);
    
    public void clean();
}
