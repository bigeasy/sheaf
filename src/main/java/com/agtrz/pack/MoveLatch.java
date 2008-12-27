package com.agtrz.pack;

final class MoveLatch
{
    private final boolean terminal;
    
    private final boolean user;

    private final Move move;

    private boolean locked;
    
    MoveLatch next;
    
    public MoveLatch(boolean user)
    {
        this.terminal = true;
        this.user = user;
        this.locked = false;
        this.move = null;
    }

    public MoveLatch(Move move, boolean user)
    {
        this.move = move;
        this.locked = true;
        this.user = user;
        this.terminal = false;
    }

    public boolean isTerminal()
    {
        return terminal;
    }
    
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

    public Move getMove()
    {
        return move;
    }
    
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