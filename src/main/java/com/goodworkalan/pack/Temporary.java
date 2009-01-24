package com.goodworkalan.pack;

import java.nio.ByteBuffer;


/**
 * Acts a both a journal entry to write the temporary reference node and a
 * rollback strategy for the mutator to invoke during a rollback. This is why
 * the mutator holds onto a list of these temporary journal entries, so that
 * during a rollback, the temporary reference node can be put back into the set
 * of available temporary reference nodes kept by the pager.
 * <p>
 * The temporary reference node is itself referenced by an address, so that it
 * can be relocated like any other block.
 *
 * @author Alan Gutierrez
 */
final class Temporary
extends Operation
{
    /** The address of the temporary block. */
    private long address;
    
    /** The address of the temporary reference node. */
    private long temporary;
    
    /**
     * Construct an empty instance that can be populated with the
     * <code>read</code> method.
     */
    public Temporary()
    {
    }
    
    /**
     * Construct an instance of a temporary allocation journal entry that will
     * write the allocation of the temporary black at the given address into the
     * temporary reference node at the given address.
     *
     * @param address The address of the temporary block.
     * @param temporary The address of the temporary reference node.
     */
    public Temporary(long address, long temporary)
    {
        this.address = address;
        this.temporary = temporary;
    }
    
    /**
     * Write the temporary block address into the temporary node reference.
     * 
     * @param player The journal player.
     */
    @Override
    public void commit(Player player)
    {
        player.getPager().setTemporary(address, temporary, player.getDirtyPages());
    }
    
    /**
     * Called by a mutator during a rollback to return the temporary reference
     * node to the set of free temporary refeence nodes maintained by the pager.
     *
     * @param pager The pager.
     */
    public void rollback(Pager pager)
    {
        pager.rollbackTemporary(address, temporary);
    }

    /**
     * Return the length of this operation in the journal including the type
     * flag.
     * 
     * @return The length of this operation in the journal.
     */
    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.ADDRESS_SIZE * 2;
    }
    
    /**
     * Write the operation type flag and the operation data to the given byte
     * buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.TEMPORARY);
        bytes.putLong(address);
        bytes.putLong(temporary);
    }
    
    /**
     * Read the operation data but not the preceeding operation type flag from
     * the byte buffer.
     * 
     * @param bytes
     *            The byte buffer.
     */
    @Override
    public void read(ByteBuffer bytes)
    {
        address = bytes.getLong();
        temporary = bytes.getLong();
    }
}
