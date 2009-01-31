package com.goodworkalan.sheaf;

/**
 * A latch that protects a move so that the change in position is not applied
 * to the page references of a mutation until the change in position
 * is final.
 * 
 * @author Alan Gutierrez
 */
final class MoveLatch
{
    /**
     * True if the move latch node is really the head of a list of appended move latch
     * nodes.
     */
    private final boolean head;
    
    /**
     * True if the page referenced by the move in the move latch node is a 
     * user page. 
     */
    private final boolean user;

    /**
     * The move structure containing the positions a page moved from and to.
     */
    private final Move move;

    private boolean locked;
    
    /** The next move latch node in the per Pack linked list of move latches. */
    MoveLatch next;
    
    /**
     * Create the last node in a linked list of move nodes.
     * 
     * @param user
     *            True if the linked list of move nodes referenced user pages.
     */
    public MoveLatch(boolean user)
    {
        this.head = true;
        this.user = user;
        this.locked = false;
        this.move = null;
    }

    public MoveLatch(Move move, boolean user)
    {
        this.move = move;
        this.locked = true;
        this.user = user;
        this.head = false;
    }

    /**
     * Return true if the move latch node is really the head of a list of
     * appended move latch nodes. 
     * 
     * @return True if the move latch is really the head of a list of append
     * move latch nodes. 
     */
    public boolean isHead()
    {
        return head;
    }
    
    /**
     * Return true if the page referenced by the move in the move latch node is
     * a user page.
     * 
     * @return True if the page referenced is a user page.
     */
    public boolean isUser()
    {
        return user;
    }

    public synchronized boolean isLocked()
    {
        return locked;
    }
    
    public synchronized void unlatch()
    {
        locked = false;
        notifyAll();
    }

    /**
     * Check if the latch has been released and if not, wait until the latch is
     * released. The thread will enter the area guarded by the latch.
     * 
     * @return True of the latch was locked when the method was called.
     */
    public synchronized boolean enter()
    {
        boolean wasLocked = locked;
        while (locked)
        {
            try
            {
                wait();
            }
            catch (InterruptedException e)
            {
            }
        }
        return wasLocked;
    }

    /**
     * The move structure containing the positions a page moved from and to.
     */
    public Move getMove()
    {
        return move;
    }
    
    /**
     * Extend the move list by appending the next move latch to this latch. This
     * latch must be the last latch node in the per pack linked list of latch
     * nodes. The last latch
     * 
     * @param next
     */
    public void extend(MoveLatch next)
    {
        assert this.next == null;
    
        this.next = next;
    }

    public MoveLatch getNext()
    {
        return next;
    }

    public MoveLatch getLast()
    {
        MoveLatch iterator = this;
        while (iterator.next != null)
        {
            iterator = iterator.next;
        }
        return iterator;
    }
}