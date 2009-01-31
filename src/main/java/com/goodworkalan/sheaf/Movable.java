package com.goodworkalan.sheaf;

/**
 * A reference to a file position adjusts the file position based on the
 * page moves since the creation of the reference.
 *
 * @author Alan Gutierrez
 */
class Movable
{
    /** The root move node.  */
    private final MoveNode moveNode;
    
    /** The file position to track. */
    private final long position;
    
    /** The number of moves to skip (0 or 1). */
    private final int skip;
    
    /**
     * Create a reference to a file position that will adjust the
     * position based on the file moved appeneded to the given move
     * node, skipping the number of move nodes given by skip.
     *
     * @param moveNode The head of a list of move nodes.
     * @param position The file position to track.
     * @param skip The number of moves to skip (0 or 1). 
     */
    public Movable(MoveNode moveNode, long position, int skip)
    {
        this.moveNode = moveNode;
        this.position = position;
        this.skip = skip;
    }
    
    /**
     * Return the file position referenced by this movable reference
     * adjusting the file position based on page moves since the
     * creation of the reference.
     *
     * @param pager The pager.
     */
    public long getPosition(Pager pager)
    {
        return pager.adjust(moveNode, position, skip);
    }
}

/* vim: set tw=80 : */
