package com.agtrz.pack;

/**
 * A superfluous little class.
 */
final class Boundary
{
    private final int pageSize;
    
    private long position;
    
    public Boundary(int pageSize, long position)
    {
        this.pageSize = pageSize;
        this.position = position;
    }
    
    public long getPosition()
    {
        return position;
    }
    
    public void increment()
    {
        position += pageSize;
    }
    
    public void decrement()
    {
        position -= pageSize;
    }
}