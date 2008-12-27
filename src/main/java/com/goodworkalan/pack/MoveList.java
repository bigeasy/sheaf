package com.goodworkalan.pack;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class MoveList
{
    private final MoveRecorder recorder;

    private final ReadWriteLock readWriteLock;
    
    private final List<MoveLatch> listOfMoveLatches;
    
    private MoveLatch headOfMoves;
    
    private boolean wasLocked;
    
    private boolean skipping;
    
    public MoveList()
    {
        this.recorder = new NullMoveRecorder();
        this.headOfMoves = new MoveLatch(false);
        this.readWriteLock = new ReentrantReadWriteLock();
        this.listOfMoveLatches = new ArrayList<MoveLatch>();
    }

    public MoveList(MoveRecorder recorder, MoveList listOfMoves)
    {
        this.recorder = recorder;
        this.headOfMoves = listOfMoves.headOfMoves;
        this.readWriteLock = listOfMoves.readWriteLock;
        this.listOfMoveLatches = new ArrayList<MoveLatch>(listOfMoves.listOfMoveLatches);
    }
    
    public void add(MoveLatch next)
    {
        readWriteLock.writeLock().lock();
        try
        {
            MoveLatch iterator = headOfMoves;
            while (iterator.getNext() != null)
            {
                iterator = iterator.getNext();
                if (iterator.isUser())
                {
                    if (iterator.isTerminal())
                    {
                        listOfMoveLatches.clear();
                    }
                    else
                    {
                        listOfMoveLatches.add(iterator);
                    }
                }
            }
            iterator.extend(next);
            
            headOfMoves = iterator;
        }
        finally
        {
            readWriteLock.writeLock().unlock();
        }
    }
    
    public void skip(MoveLatch skip)
    {
        wasLocked = false;
        skipping = false;
        readWriteLock.readLock().lock();
        try
        {
            for (;;)
            {
                if (headOfMoves.next == null)
                {
                    break;
                }
                advance(skip);
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
                if (headOfMoves.next == null)
                {
                    return guarded.run(listOfMoveLatches);
                }
                advance(null);
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
                if (headOfMoves.next == null)
                {
                    guarded.run(listOfMoveLatches);
                    break;
                }
                advance(null);
            }
        }
        finally
        {
            readWriteLock.readLock().unlock();
        }
    }

    private void advance(MoveLatch skip)
    {
        headOfMoves = headOfMoves.next;
        if (headOfMoves.isTerminal())
        {
            if (headOfMoves.isUser())
            {
                if (wasLocked)
                {
                    throw new IllegalStateException();
                }
                listOfMoveLatches.clear();
            }
            skipping = skip == headOfMoves;
        }
        else
        {
            if (recorder.involves(headOfMoves.getMove().getFrom()))
            {
                if (skipping)
                {
                    if (headOfMoves.isUser() && headOfMoves.isLocked())
                    {
                        wasLocked = true;
                    }
                }
                else if (headOfMoves.enter())
                {
                    if (headOfMoves.isUser())
                    {
                        wasLocked = true;
                    }
                }
                recorder.record(headOfMoves.getMove(), false);
            }
            else
            {
                if (headOfMoves.isUser())
                {
                    if (headOfMoves.isLocked())
                    {
                        wasLocked = true;
                    }
                    listOfMoveLatches.add(headOfMoves);
                }
            }
        }
    }
}