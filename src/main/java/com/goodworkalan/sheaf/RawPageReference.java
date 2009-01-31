package com.goodworkalan.sheaf;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * A weak reference to a raw page that will queue itself into a
 * <code>ReferenceQueue</code> that will remove the raw page reference from the
 * page by position map of a {@link Sheaf}.
 */
final class RawPageReference
extends WeakReference<RawPage>
{
    /** The file position of the raw page. */
    private final long position;

    /**
     * Create a page reference for the raw page and add the referenc to the
     * specified reference queue.
     * 
     * @param page
     *            The raw page.
     * @param queue
     *            The reference queue.
     */
    public RawPageReference(RawPage page, ReferenceQueue<RawPage> queue)
    {
        super(page, queue);
        this.position = page.getPosition();
    }

    /**
     * The file position of the raw page.
     * 
     * @return The file position.
     */
    public long getPosition()
    {
        return position;
    }
}
