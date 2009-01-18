package com.goodworkalan.pack;

import java.util.List;

/**
 * Perform a update of the pages in a Pack file that is guarded against page
 * movement. Guarded is passed to a <code>Mutator</code> object's
 * <code>MoveList</code>. The <code>MoveList</code> will call the {@link #run()}
 * method after recording all the moves that effect the positions recorded by
 * the <code>MoveRecoder</code> assigned to the <code>MoveList</code>. The
 * <code>MoveList</code> will all the {@link MoveLatch#enter()} method of moves
 * that effect the recorded positions, waiting for those moves to complete.
 * <p>
 * The {@link #run()} method takes a list of <code>MoveLatch</code> objects that
 * represent the most recent list of moves of user pages to accommodate new
 * address pages. The {@link Mutator#write(long, java.nio.ByteBuffer)} and
 * {@link Mutator#free(long)} methods need to check the most recent list of
 * moves ...
 * 
 * FIXME Why the most recent list? Why not every move?
 * 
 * @author Alan Gutierrez
 */
class Guarded
{
    public void run(List<MoveLatch> userMoveLatches)
    {
    }
}
