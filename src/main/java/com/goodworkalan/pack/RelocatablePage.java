package com.goodworkalan.pack;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.Checksum;


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
    
    public void checksum(Checksum checksum)
    {
        throw new UnsupportedOperationException();
    }
    
    public boolean verifyChecksum(RawPage rawPage, Recovery recovery)
    {
        throw new UnsupportedOperationException();
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
        // TODO Not enough locking?
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