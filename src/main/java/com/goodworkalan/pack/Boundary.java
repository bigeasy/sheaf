package com.goodworkalan.pack;

/**
 * Tracks one of the two boundaries between the three sections of the Pack file.
 * 
 * @author Alan Gutierrrez
 */
final class Boundary
{
    /** The size of a page in the Pack.  */
    private final int pageSize;
    
    /** The position of the boundary.  */
    private long position;
    
    /**
     * Create a boundary tracker for pages of the given size that is set at the
     * given position.
     * 
     * @param pageSize
     *            The size of a page used to increment and decrement the
     *            position.
     * @param position
     *            The initial position.
     */
    public Boundary(int pageSize, long position)
    {
        this.pageSize = pageSize;
        this.position = position;
    }
    
    /**
     * Get the position of the boundary.
     * 
     * @return The position of the boundary.
     */
    public long getPosition()
    {
        return position;
    }
    
    /**
     * Increment the boundary position by one page.
     */
    public void increment()
    {
        position += pageSize;
    }
    
    /**
     * Decrement the boundary position by one page.
     */
    public void decrement()
    {
        position -= pageSize;
    }
}
