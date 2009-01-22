package com.goodworkalan.pack;

import java.nio.ByteBuffer;

/**
 * Adds a relocatable page move to the list of page moves recorded in the
 * player before replaying a journal.
 * 
 * @author Alan Gutierrez
 *
 */
final class AddMove
extends Operation
{
    /** The relocatable page move to restore. */
    private Move move;

    /**
     * Default constructor builds an empty instance that can be populated
     * with the <code>read</code> method.
     */
    public AddMove()
    {
    }

    /**
     * Construct an instance that will write the relocatable page move to the
     * journal using the <code>write</code> method.
     * 
     * @param move
     *            The page move.
     */
    public AddMove(Move move)
    {
        this.move = move;
    }

    /**
     * Restore a page move to the list of moves in the given player.
     * 
     * @param player
     *            The journal player.
     */
    @Override
    public void commit(Player player)
    {
        player.getMoveList().add(move);
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
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 2;
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
        move = new Move(bytes.getLong(), bytes.getLong());
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
        bytes.putShort(Pack.ADD_MOVE);
        bytes.putLong(move.getFrom());
        bytes.putLong(move.getTo());
    }
}