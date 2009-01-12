package com.goodworkalan.pack;

import java.nio.ByteBuffer;


/**
 * Copy does not wait on mirrored pages, since it is called only on 
 * pages that it has vacuumed and mirrored. In the cases of address
 * expansion, copy will be draining a mirrored page using free. Any
 * writers or readers will block and when the block is not found, they
 * will dereference the address page again, to determine the new location
 * of the block.
 * <p>
 * This use means the mirror flag should block for address page relocation
 * through the entire commit, whereas with an ordinary commit, we can
 * allow concurrent writes after the vacuum commit.
 */
final class Copy
extends Operation
{
    private long address;

    private long from;
    
    private long to;
    
    public Copy()
    {
    }

    public Copy(long address, long from, long to)
    {
        this.address = address;
        this.from = from;
        this.to = to;
    }
    
    @Override
    public void commit(Player player)
    {
        Pager pager = player.getPager();
        pager.getAddressLocker().bide(address);
        InterimPage interim = pager.getPage(from, InterimPage.class, new InterimPage());
        UserPage user = pager.getPage(to, UserPage.class, new UserPage());
        interim.copy(address, user, player.getDirtyPages());
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 3;
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.COPY);
        bytes.putLong(address);
        bytes.putLong(from);
        bytes.putLong(to);
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        this.address = bytes.getLong();
        this.from = bytes.getLong();
        this.to = bytes.getLong();
    }
}