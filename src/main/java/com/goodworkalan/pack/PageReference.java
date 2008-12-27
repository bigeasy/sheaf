package com.goodworkalan.pack;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

final class PageReference
extends WeakReference<RawPage> implements Positionable
{
    private final Long position;

    public PageReference(RawPage page, ReferenceQueue<RawPage> queue)
    {
        super(page, queue);
        this.position = new Long(page.getPosition());
    }

    public Long getPosition()
    {
        return position;
    }
}