package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;

class RelocatablePage
implements Page
{
    private RawPage rawPage;
    
    public void create(RawPage rawPage, DirtyPageSet dirtyPages)
    {
        this.rawPage = rawPage;
        rawPage.setPage(this);
    }

    public void load(RawPage rawPage)
    {
        this.rawPage = rawPage;
        rawPage.setPage(this);
    }
    
    public RawPage getRawPage()
    {
        return rawPage;
    }
    
    /**
     * Relocate a page from one position to another writing it out
     * immediately. This method does not use a dirty page map, the page
     * is written immediately.
     * 
     * @param to
     *            The position where the page will be relocated.
     */
    public void relocate(long to)
    {
        RawPage rawPage = getRawPage();
        Pager pager = rawPage.getPager();
        ByteBuffer bytes = rawPage.getByteBuffer();
        bytes.clear();
        try
        {
            pager.getDisk().write(pager.getFileChannel(), bytes, to);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        rawPage.setPosition(to);
    }
}