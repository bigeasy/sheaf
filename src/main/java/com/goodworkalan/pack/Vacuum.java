package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class Vacuum
extends Operation
{
    private long from;
    
    private long to;
    
    private long checksum;
    
    private int offset;
    
    public Vacuum()
    {
    }

    /**
     * Create a vacuum that copies blocks the given interim block page to the
     * given user block page starting at the given block offset and checks the
     * result against the given checksum.
     * 
     * @param mirror
     *            The interim page that mirrors the user page, the block offset,
     *            and the checksum of result.
     * @param to
     *            The user page to vacuum.
     */
    public Vacuum(Mirror mirror, BlockPage to)
    {
        this.from = mirror.getMirrored().getRawPage().getPosition();
        this.to = to.getRawPage().getPosition();
        this.checksum = mirror.getChecksum();
        this.offset = mirror.getOffset();
    }

    /**
     * Called by the {@link VacuumCheckpoint} journal entry to actually perform
     * the vacuum by calling
     * {@link InterimPage#vacuum(UserPage, DirtyPageSet, int, long) vacuum} on
     * the interim page that stores the mirror of the vacuumed user page.
     * 
     * @param player
     *            The journal player.
     */
    public void vacuum(Player player)
    {
        InterimPage mirrored = player.getPager().getPage(from, InterimPage.class, new InterimPage());
        UserPage user = player.getPager().getPage(to, UserPage.class, new UserPage());
        mirrored.vacuum(user, player.getDirtyPages(), offset, checksum);
    }

    /**
     * Adds this add vacuum journal operation to the set of add vacuum
     * operations, since add vacuum is used to invoke the
     * {@link InterimPage#vacuum(UserPage, DirtyPageSet, int, long) vacuum}
     * method of the interim page.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void commit(Player player)
    {
        player.getVacuumSet().add(this);
    }
    
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 3 + Pack.COUNT_SIZE;
    }
    
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.ADD_VACUUM);
        bytes.putLong(from);
        bytes.putLong(to);
        bytes.putLong(checksum);
        bytes.putInt(offset);
    }
    
    @Override
    public void read(ByteBuffer bytes)
    {
        this.from = bytes.getLong();
        this.to = bytes.getLong();
        this.checksum = bytes.getLong();
        this.offset = bytes.getInt();
    }
}