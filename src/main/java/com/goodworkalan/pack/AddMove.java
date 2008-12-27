package com.goodworkalan.pack;

import java.nio.ByteBuffer;


final class AddMove
extends Operation
{
    private Move move;

    public AddMove()
    {
    }

    public AddMove(Move move)
    {
        this.move = move;
    }
    
    @Override
    public void commit(Player player)
    {
        player.getMoveList().add(move);
    }

    @Override
    public int length()
    {
        return Pack.FLAG_SIZE + Pack.POSITION_SIZE * 2;
    }

    @Override
    public void read(ByteBuffer bytes)
    {
        move = new Move(bytes.getLong(), bytes.getLong());
    }

    @Override
    public void write(ByteBuffer bytes)
    {
        bytes.putShort(Pack.ADD_MOVE);
        bytes.putLong(move.getFrom());
        bytes.putLong(move.getTo());
    }
}