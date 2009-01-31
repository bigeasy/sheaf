package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;


final class UserPage extends BlockPage
{
    /**
     * True if the page is in the midst of a vacuum and should not be written to.
     */
    private boolean mirrored;

    protected int getDiskCount()
    {
        return count | Pack.COUNT_MASK;
    }
    
    protected int getDiskCount(int count)
    {
        if ((count & Pack.COUNT_MASK) == 0)
        {
            throw new PackException(Pack.ERROR_CORRUPT);
        }
        return count & ~Pack.COUNT_MASK;
    }

    /**
     * Called in two peculiar places. Before free and before write. Hidden
     * in the code. Not called for copies.
     */
    public synchronized void waitOnMirrored()
    {
        while (mirrored)
        {
            try
            {
                wait();
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    /**
     * Mirror the user block page to the given interim block page.
     * <p>
     * The vacuum parameter indicates that mirroring should begin at the first
     * allocated block beyond the first free block. If there are no free blocks
     * to vacuum, then <code>mirror</code> returns null. If the interim
     * parameter is null, then the <code>pager</code> will be used to allocate
     * an <code>InterimPage</code> if vacuum is necessary.
     * 
     * @param vacuum
     *            If true, mirror only if there are free blocks between
     *            allocated blocks, if false, mirror from the first block.
     * @param pager
     *            The pager to use to allocate an interim page.
     * @param interim
     *            The interim page to which this page is mirrored, or null for
     *            only if needed allocation.
     * @param dirtyPages
     *            The set of dirty pages.
     * @return A mirror object with information on mirrored interim page given
     *         or the mirrored page allocated, or null if no interim page was
     *         given nor allocated.
     */
    public synchronized Mirror mirror(boolean vacuum, Pager pager, InterimPage interim, DirtyPageSet dirtyPages)
    {
        int offset = vacuum ? -1 : 0;
        
        Mirror mirror = null;
        
        assert ! mirrored;
        
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            int block = 0;
            while (block != count)
            {
                int size = getBlockSize(bytes);
                if (size < 0)
                {
                    if (offset == -1)
                    {
                        offset = block;
                    }
                    advance(bytes, size);
                }
                else
                {
                    block++;
                    if (offset == -1)
                    {
                        advance(bytes, size);
                    }
                    else
                    {
                        if (interim == null)
                        {
                            interim = pager.newInterimPage(new InterimPage(), dirtyPages);
                        }

                        assert size <= interim.getRemaining();

                        int blockSize = bytes.getInt();
                        long address = bytes.getLong();
                        
                        interim.allocate(address, blockSize, dirtyPages);

                        int userSize = blockSize - Pack.BLOCK_HEADER_SIZE;

                        bytes.limit(bytes.position() + userSize);
                        interim.write(address, bytes, dirtyPages);
                        bytes.limit(bytes.capacity());
                    }
                } 
            }
            
            if (interim != null)
            {
                long checksum = getChecksum(dirtyPages.getChecksum());
                mirror = new Mirror(interim, offset, checksum);
            }
        }
        
        mirrored = mirror != null;

        return mirror;
    }
    
    public synchronized void unmirror()
    {
        mirrored = false;
        notifyAll();
    }
    
    public void copy(long address, ByteBuffer block, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            RawPage rawPage = getRawPage();
            Pager pager = rawPage.getPager();
            AddressPage addresses = pager.getPage(address, AddressPage.class, new AddressPage());
            long position = addresses.dereference(address);
            if (position != getRawPage().getPosition())
            {
                if (position == 0L)
                {
                    throw new IllegalStateException();
                }
                if (position != Long.MAX_VALUE)
                {
                    UserPage blocks = pager.getPage(position, UserPage.class, new UserPage());
                    blocks.free(address, dirtyPages);
                }
                addresses.set(address, getRawPage().getPosition(), dirtyPages);
            }
            
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int size = bytes.getInt();
                
                if (size != block.remaining() + Pack.BLOCK_HEADER_SIZE)
                {
                    throw new IllegalStateException();
                }
                
                if (bytes.getLong() != address)
                {
                    throw new IllegalStateException();
                }
                
                getRawPage().invalidate(bytes.position(), block.remaining());
                bytes.put(block);
            }
            else
            {
                if (block.remaining() + Pack.BLOCK_HEADER_SIZE > bytes.remaining())
                {
                    throw new IllegalStateException();
                }
                
                getRawPage().invalidate(bytes.position(), block.remaining() + Pack.BLOCK_HEADER_SIZE);
                
                remaining -= block.remaining() + Pack.BLOCK_HEADER_SIZE;
                
                bytes.putInt(block.remaining() + Pack.BLOCK_HEADER_SIZE);
                bytes.putLong(address);
                bytes.put(block);
                
                count++;
                
                getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
                bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());
            }

            dirtyPages.add(getRawPage());
        }
    }
    
    public boolean free(long address, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int offset = bytes.position();

                int size = bytes.getInt();
                if (size > 0)
                {
                    size = -size;
                }

                getRawPage().invalidate(offset, Pack.COUNT_SIZE);
                bytes.putInt(offset, size);
                
                count--;
                getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
                bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());

                dirtyPages.add(getRawPage());
                return true;
            }
        }
        return false;
    }
}