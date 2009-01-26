package com.goodworkalan.pack;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * A weak reference to a raw page that will queue itself into a
 * <code>ReferenceQueue</code> that will remove the raw page reference
 * from the page by position map of a {@link Pager}.
 *
 * TODO Rename RawPageReference.
 */
final class PageReference
extends WeakReference<RawPage>
{
    /**
     * The position of the page.
     */
    private final long position;

    /**
     * Create a page reference for the raw page and add the referenc to
     * the specified reference queue.
     *
     * @param page The raw page.
     * @param queue The reference queue.
     */
    public PageReference(RawPage page, ReferenceQueue<RawPage> queue)
    {
        super(page, queue);
        this.position = page.getPosition();
    }

    /**
     * The file position of the raw page.
     */
    public long getPosition()
    {
        return position;
    }
}
