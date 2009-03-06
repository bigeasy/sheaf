package com.goodworkalan.sheaf;


public interface Dirtyable
{
    public int getLength();

    public void dirty();
    
    public void dirty(int offset, int length);
}
