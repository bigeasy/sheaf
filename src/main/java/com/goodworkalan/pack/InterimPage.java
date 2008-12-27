package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class InterimPage extends BlockPage
{
    protected int getDiskCount()
    {
        return count;
    }
    
    protected int getDiskCount(int count)
    {
        if ((count & Pack.COUNT_MASK) != 0)
        {
            throw new Danger(Pack.ERROR_CORRUPT);
        }
        return count;
    }

    /**
     * Allocate a block that is referenced by the specified address.
     * 
     * @param address
     *            The address that will reference the newly allocated block.
     * @param blockSize
     *            The full block size including the block header.
     * @param dirtyPages
     *            A dirty page map to record the block page if it changes.
     * @return True if the allocation is successful.
     */
    public void allocate(long address, int blockSize, DirtyPageMap dirtyPages)
    {
        if (blockSize < Pack.BLOCK_HEADER_SIZE)
        {
            throw new IllegalArgumentException();
        }

        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            boolean found = false;
            int block = 0;
            // FIXME Not finding anymore. That's taken care of in commit.
            while (block != count && !found)
            {
                int size = getBlockSize(bytes);
                if (size > 0)
                {
                    block++;
                    if(getAddress(bytes) == address)
                    {
                        found = true;
                    }
                }
                bytes.position(bytes.position() + Math.abs(size));
            }
    
            if (!found)
            {
                getRawPage().invalidate(bytes.position(), blockSize);
    
                bytes.putInt(blockSize);
                bytes.putLong(address);
    
                count++;
                remaining -= blockSize;
    
                bytes.clear();
                bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());
                getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
    
                dirtyPages.add(getRawPage());
            }
        }
    }

    public void vacuum(UserPage user, DirtyPageMap dirtyPages, int offset, long checksum)
    {
        if (offset > user.count)
        {
            throw new IllegalStateException();
        }
        ByteBuffer bytes = user.getBlockRange();
        int block = 0;
        while (block < offset)
        {
            int blockSize = user.getBlockSize(bytes);
            assert blockSize > 0;
            user.advance(bytes, blockSize);
            block++;
        }
        if (user.count - block != count)
        {
            throw new IllegalStateException();
        }
        user.count = block;
        for (long address : getAddresses())
        {
            copy(address, user, dirtyPages);
        }
        if (checksum != user.getChecksum(dirtyPages.getChecksum()))
        {
            throw new IllegalStateException();
        }
    }

    public void copy(long address, UserPage user, DirtyPageMap dirtyPages)
    {
        // FIXME Locking a lot. Going to deadlock?
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int offset = bytes.position();
                
                int blockSize = bytes.getInt();

                bytes.position(offset + Pack.BLOCK_HEADER_SIZE);
                bytes.limit(offset + blockSize);

                user.copy(address, bytes.slice(), dirtyPages);

                bytes.limit(bytes.capacity());
            }
        }
    }
    
    public void write(long address, DirtyPageMap dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getRawPage().getByteBuffer();
            if (seek(bytes, address))
            {
                int blockSize = getBlockSize(bytes);
                bytes.limit(bytes.position() + blockSize);
                bytes.position(bytes.position() + Pack.BLOCK_HEADER_SIZE);
                

                Pager pager = getRawPage().getPager();
                AddressPage addresses = pager.getPage(address, new AddressPage());
                long lastPosition = 0L;
                for (;;)
                {
                    long actual = addresses.dereference(address);
                    if (actual == 0L || actual == Long.MAX_VALUE)
                    {
                        throw new Danger(Pack.ERROR_READ_FREE_ADDRESS);
                    }
                    
                    if (actual != lastPosition)
                    {
                        UserPage user = pager.getPage(actual, new UserPage());
                        user.waitOnMirrored();
                        synchronized (user.getRawPage())
                        {
                            if (user.write(address, bytes, dirtyPages))
                            {
                                break;
                            }
                        }
                        lastPosition = actual;
                    }
                    else
                    {
                        throw new IllegalStateException();
                    }
                }

                bytes.limit(bytes.capacity());
            }
        }
    }
    
    public void free(long address, DirtyPageMap dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getBlockRange();
            int blockSize = 0;
            int block = 0;
            while (block < count)
            {
                blockSize = getBlockSize(bytes);

                assert blockSize > 0;

                if (getAddress(bytes) == address)
                {
                    break;
                }

                advance(bytes, blockSize);

                block++;
            }

            assert block != count;
            
            int to = bytes.position();
            advance(bytes, blockSize);
            int from = bytes.position();
            
            remaining += (from - to);

            block++;

            while (block < count)
            {
                blockSize = getBlockSize(bytes);
                
                assert blockSize > 0;
                
                advance(bytes, blockSize);
                
                block++;
            }
            
            int length = bytes.position() - from;
            
            for (int i = 0; i < length; i++)
            {
                bytes.put(to + i, bytes.get(from + i));
            }
            
            if (length != 0)
            {
                getRawPage().invalidate(to, length);
            }

             count--;

            getRawPage().invalidate(Pack.CHECKSUM_SIZE, Pack.COUNT_SIZE);
            bytes.putInt(Pack.CHECKSUM_SIZE, getDiskCount());
        }
    }
}