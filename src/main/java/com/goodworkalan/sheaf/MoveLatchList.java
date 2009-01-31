package com.goodworkalan.sheaf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A linked list of move latches used to block pages from reading pages while
 * they are moving.
 * <p>
 * Pages are moved during during commit or address region expansion. To prevent
 * mutators from writing blocks to moving pages, a linked list of move latches
 * are appended to this master linked list of move latches.
 * <p>
 * Mutators hold iterators over the linked list. To perform an action that will
 * write to a user or interim page, the mutator will call one of the
 * {@link MoveLatchIterator#mutate(Guarded) mutate} methods of the
 * {@link MoveLatchIterator}. That method will iterate over the move latches,
 * and if it encounters a page involved in its mutation, it adjusts its page
 * references, and waits on the latch by calling {@link MoveLatch#enter()}.
 * 
 * @see MoveLatchIterator
 */
public class MoveLatchList
{
    /**
     * A read write lock that protects the move list itself synchronizing
     * the shared iteration of the latches or the exclusive addition of
     * new latches.
     */
    private final ReadWriteLock iterateAppendLock;

    /** The head of the move latch list. */
    private MoveLatch headMoveLatch;
    
    /**
     * A list of the last series of user page moves.
     */
    private final List<MoveLatch> userMoveLatches;

    /**
     * Construct the main move latch list that is managed by the pager.
     */
    public MoveLatchList()
    {
        this.headMoveLatch = new MoveLatch(false);
        this.iterateAppendLock = new ReentrantReadWriteLock();
        this.userMoveLatches = new ArrayList<MoveLatch>();
    }
    
    public MoveLatchIterator newIterator(MoveRecorder recorder)
    {
        iterateAppendLock.readLock().lock();
        try
        {
            return new MoveLatchIterator(recorder, iterateAppendLock, headMoveLatch, userMoveLatches);
        }
        finally
        {
            iterateAppendLock.readLock().unlock();
        }
    }

    /**
     * This method is only ever called on the <code>MoveList</code> contained in
     * the <code>Pager</code>.
     * 
     * @param next
     *            A list of move latch nodes to append to the per pager move
     *            list.
     */
    public void add(MoveLatch next)
    {
        iterateAppendLock.writeLock().lock();
        try
        {
            MoveLatch latch = headMoveLatch;
            while (latch.getNext() != null)
            {
                latch = latch.getNext();
                
                // If the latch references a user page, then we need to
                // record it in the list of user move latches.

                // There is only ever one series of user page moves,
                // moving to create space for address pages. If we see
                // the head of a chain of user latch nodes 

                if (latch.isUser())
                {
                    if (latch.isHead())
                    {
                        userMoveLatches.clear();
                    }
                    else
                    {
                        userMoveLatches.add(latch);
                    }
                }
            }
            latch.extend(next);
            
            headMoveLatch = latch;
        }
        finally
        {
            iterateAppendLock.writeLock().unlock();
        }
    }
}
