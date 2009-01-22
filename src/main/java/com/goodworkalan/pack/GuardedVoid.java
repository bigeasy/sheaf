package com.goodworkalan.pack;

import java.util.List;

/**
 * Implementation of a modification to the pages in a Pack file that returns no
 * value and is guarded against page movement. Implementations of
 * <code>GuardedVoid</code> are passed to a <code>MoveLatchList</code>. The
 * <code>MoveLatchList</code> will call the {@link #run run} method passing a
 * list of the move latches guarding the most recent moves of user pages. If the
 * run method dereferences an address page, it must use the list of move recent
 * move latches to adjust the reference to the address page.
 * 
 * @author Alan Gutierrez
 */
interface GuardedVoid
{
    /**
     * Perform a modification to the pages in a Pack file that is guarded
     * against page movement. If the method dereferences an address in an
     * address page, the dereferenced position must be adjusted using the list
     * of user move latches given.
     * 
     * @param userMoveLatches
     *            A list of move latches of the most recent move of user pages.
     */
    public void run(List<MoveLatch> userMoveLatches);
}

/* vim: set tw=80 : */
