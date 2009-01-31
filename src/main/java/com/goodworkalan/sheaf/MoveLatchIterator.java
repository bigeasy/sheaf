package com.goodworkalan.sheaf;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * A linked list of latches that guard page moves.
 * <p>
 * The move list is extended by appending a list of move latch nodes, so that
 * the move latch nodes in the move list itself are chains of related moves.
 * This means that we can {@link #skip()} a particular sub linked list of moves,
 * so that a thread can skip the latches that it adds itself.
 * <h2>Position</h2>
 * The list of user page moves is passed to the {@link GuardedVoid} and
 * {@link Guarded} so that {@link Mutator#free(long)} and
 * {@link Mutator#write(long, java.nio.ByteBuffer)} can determine if they are
 * freeing or writing to a page that has been moved. This is a special case.
 * <p>
 * Pages allocated by a <code>Mutator</code> are tracked using a {@link Movable}
 * at the outset. The <code>Movable</code> will adjust the position it
 * references according to the latch node linked list that starts from the node
 * that was the tail when the position was allocated.
 * <p>
 * Pages that are referenced by the client programmer that were not created by
 * the <code>Mutator</code>
 */
final class MoveLatchIterator
{
    /**
     * Records the moves encountered while running guarded methods through this
     * move latch list.
     */
    private final MoveRecorder recorder;

    /**
     * A read write lock that protects the move list itself synchronizing
     * the shared iteration of the latches or the exclusive addition of
     * new latches.
     */
    private final ReadWriteLock iterateAppendLock;
    
    /**
     * A list of the last series of user page moves.
     */
    private final List<MoveLatch> userMoveLatches;
    
    /** The head of the move latch list. */
    private MoveLatch headMoveLatch;

    /**
     * This is saying that there is only ever supposed to be one string of move
     * nodes on user pages that has an locked latch, asserting that only one
     * thread at a time is moving user pages.
     */
    private boolean userPageWasLocked;

    /**
     * If true, we skip testing to see if the moved page is of interest to the
     * move recorder until the next start of a list of move latches is
     * encountered.
     */
    private boolean skipping;
    
    /**
     * Construct a move latch list that records move using the given move
     * recorder and begins at the head of the given move latch list.
     */
    public MoveLatchIterator(MoveRecorder recorder, ReadWriteLock iterateAppendLock, MoveLatch headMoveLatch, List<MoveLatch> userMoveLatches)
    {
        this.recorder = recorder;
        this.headMoveLatch = headMoveLatch;
        this.iterateAppendLock = iterateAppendLock;
        this.userMoveLatches = new ArrayList<MoveLatch>(userMoveLatches);
    }

    /**
     * Advance the move latch list head pointer, when the specific list of moves
     * that begins with the given move latch node is encountered, do not invoke
     * the enter method for any of the latches. This method is used by mutators
     * to skip over the latches that they themselves have added.
     * 
     * @param skip
     *            The head move latch of a specific list of move latches to
     *            skip.
     */
    public void skip(MoveLatch skip)
    {
        userPageWasLocked = false;
        skipping = false;
        iterateAppendLock.readLock().lock();
        try
        {
            while (advance(skip))
            {
            }
        }
        finally
        {
            iterateAppendLock.readLock().unlock();
        }
    }

    /**
     * Locks the move latch list to prevent the addition of new moves, then
     * updates the associated move recorder with the move append to list since
     * the last call to <code>skip</code> or <code>guarded</code>, before
     * performing the given guarded action. This method will return the value
     * returned by the given guarded action.
     * 
     * @param <T>
     *            The return type of the guarded action.
     * @param guarded
     *            The guarded action to perform.
     * @return The value returned by running the given guarded action.
     */
    public <T> T mutate(final Guarded<T> guarded)
    {
        userPageWasLocked = false;
        skipping = false;
        iterateAppendLock.readLock().lock();
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
            iterateAppendLock.readLock().unlock();
        }
    }

    /**
     * Locks the move latch list to prevent the addition of new moves, then
     * updates the associated move recorder with the move append to list since
     * the last call to <code>skip</code> or <code>guarded</code>, before
     * performing the given guarded action. This version of <code>guarded</code>
     * is for action that do not return a value.
     * 
     * @param guarded
     *            The guarded action to perform.
     */
    public void mutate(GuardedVoid guarded)
    {
        userPageWasLocked = false;
        skipping = false;
        iterateAppendLock.readLock().lock();
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
            iterateAppendLock.readLock().unlock();
        }
    }

    /**
     * Advance the head latch node forward by assigning it the value of its next
     * property returning false if there are no more latch nodes.
     * <p>
     * The <code>skip</code> move latch indicates the first node in a specific
     * list of move latch nodes that will not be tested for involvement, their
     * latches will not be entered. This is used by mutators to skip the move
     * latch nodes that they themselves have appended to the per pager. list of
     * move latches.
     * 
     * @param skip
     *            The first node of a specific list of latch nodes whose that
     *            not be tested for involvement or entered.
     * 
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
                if (userPageWasLocked)
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
                        userPageWasLocked = true;
                    }
                }
                else if (headMoveLatch.enter())
                {
                    if (headMoveLatch.isUser())
                    {
                        userPageWasLocked = true;
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
                        userPageWasLocked = true;
                    }
                    userMoveLatches.add(headMoveLatch);
                }
            }
        }

        return true;
    }
}
