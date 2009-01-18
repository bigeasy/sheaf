package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A linked list of moves.
 * 
 * <p>
 * The move list is extended by appending a list of move latch nodes, so
 * that the move latch nodes in the move list itself are chains of
 * related moves.  This means that we can {@link #skip()} a particular
 * sub linked list of moves, so that a thread can skip the latches that
 * it adds itself.
 * <h2>Position</h2>
 * The list of user page moves is passed to the {@link Guarded} and
 * {@link GuardedReturnable} so that {@link Mutator#free(long)} and
 * {@link Mutator#write(long, java.nio.ByteBuffer)} can determine if
 * they are freeing or writing to a page that has been moved. This is a
 * special case.
 * <p>
 * Pages allocated by a <code>Mutator</code> are tracked using a {@link
 * Movable} at the outset. The <code>Movable</code> will adjust the
 * position it references according to the latch node linked list that
 * starts from the node that was the tail when the position was
 * allocated.
 * <p>
 * Pages that are referenced by the client programmer that were not
 * created by the <code>Mutator</code> 
 */
final class MoveList
{
    private final MoveRecorder recorder;

    /**
     * A read write lock that protects the move list itself synchronizing
     * the shared iteration of the latches or the exclusive addition of
     * new latches.
     */
    private final ReadWriteLock readWriteLock;
    
    /**
     * A list of the last series of user page moves.
     */
    private final List<MoveLatch> userMoveLatches;
    
    private MoveLatch headMoveLatch;
    
    private boolean wasLocked;
    
    private boolean skipping;
    
    public MoveList()
    {
        this.recorder = new NullMoveRecorder();
        this.headMoveLatch = new MoveLatch(false);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.userMoveLatches = new ArrayList<MoveLatch>();
    }

    public MoveList(MoveRecorder recorder, MoveList listOfMoves)
    {
        this.recorder = recorder;
        this.headMoveLatch = listOfMoves.headMoveLatch;
        this.readWriteLock = listOfMoves.readWriteLock;
        this.userMoveLatches = new ArrayList<MoveLatch>(listOfMoves.userMoveLatches);
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
        readWriteLock.writeLock().lock();
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
            readWriteLock.writeLock().unlock();
        }
    }
    
    /**
     * Advance the 
     * @param skip
     */
    public void skip(MoveLatch skip)
    {
        wasLocked = false;
        skipping = false;
        readWriteLock.readLock().lock();
        try
        {
            while (advance(skip))
            {
            }
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
    }

    public <T> T mutate(final GuardedReturnable<T> guarded)
    {
        wasLocked = false;
        skipping = false;
        readWriteLock.readLock().lock();
        try
        {
            for (;;)
            {
                if (!advance(null))
                {
                    return guarded.run(userMoveLatches);
                }
            }
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
    }
    
    public void mutate(Guarded guarded)
    {
        wasLocked = false;
        skipping = false;
        readWriteLock.readLock().lock();
        try
        {
            for (;;)
            {
                if (!advance(null))
                {
                    guarded.run(userMoveLatches);
                    break;
                }
            }
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
    }

    /**
     * Advance the head latch node forward by assigning it the value of
     * its next property returning false if there are no more latch
     * nodes.
     *
     * @param skip 
     * @return False if there are no more latches.
     */
    private boolean advance(MoveLatch skip)
    {
        if (headMoveLatch.next == null)
        {
            return false;
        }

        // Get the first move which follows the head of the move list.
        headMoveLatch = headMoveLatch.next;
        if (headMoveLatch.isHead())
        {
            if (headMoveLatch.isUser())
            {
                if (wasLocked)
                {
                    throw new IllegalStateException();
                }
                userMoveLatches.clear();
            }
            skipping = skip == headMoveLatch;
        }
        else
        {
            if (recorder.involves(headMoveLatch.getMove().getFrom()))
            {
                if (skipping)
                {
                    if (headMoveLatch.isUser() && headMoveLatch.isLocked())
                    {
                        wasLocked = true;
                    }
                }
                else if (headMoveLatch.enter())
                {
                    if (headMoveLatch.isUser())
                    {
                        wasLocked = true;
                    }
                }
                recorder.record(headMoveLatch.getMove(), false);
            }
            else
            {
                if (headMoveLatch.isUser())
                {
                    if (headMoveLatch.isLocked())
                    {
                        wasLocked = true;
                    }
                    userMoveLatches.add(headMoveLatch);
                }
            }
        }

        return true;
    }
}
