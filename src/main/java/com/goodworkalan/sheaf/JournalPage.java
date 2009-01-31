package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.zip.Checksum;


final class JournalPage
extends RelocatablePage
{
    private int offset;

    public void create(RawPage position, DirtyPageSet dirtyPages)
    {
        super.create(position, dirtyPages);

        ByteBuffer bytes = getRawPage().getByteBuffer();
        
        bytes.clear();
        bytes.putLong(0);
        bytes.putInt(0);

        getRawPage().setPage(this);
        
        dirtyPages.add(getRawPage());
        
        this.offset = Pack.JOURNAL_PAGE_HEADER_SIZE;
    }

    public void load(RawPage position)
    {
        super.load(position);
        
        this.offset = Pack.JOURNAL_PAGE_HEADER_SIZE;
    }
    
    /**
     * Checksum the entire journal page. In order to checksum only journal
     * pages I'll have to keep track of where the journal ends.
     * 
     * @param checksum The checksum algorithm.
     */
    public void checksum(Checksum checksum)
    {
        checksum.reset();
        ByteBuffer bytes = getRawPage().getByteBuffer();
        bytes.position(Pack.CHECKSUM_SIZE);
        while (bytes.position() != offset)
        {
            checksum.update(bytes.get());
        }
        bytes.putLong(0, checksum.getValue());
        getRawPage().invalidate(0, Pack.CHECKSUM_SIZE);
    }
    
    private ByteBuffer getByteBuffer()
    {
        ByteBuffer bytes = getRawPage().getByteBuffer();
        
        bytes.clear();
        bytes.position(offset);

        return bytes;
    }

    public boolean write(Operation operation, int overhead, DirtyPageSet dirtyPages)
    {
        synchronized (getRawPage())
        {
            ByteBuffer bytes = getByteBuffer();

            if (operation.length() + overhead < bytes.remaining())
            {
                getRawPage().invalidate(offset, operation.length());
                operation.write(bytes);
                offset = bytes.position();
                dirtyPages.add(getRawPage());
                return true;
            }
            
            return false;
        }
    }

    public long getJournalPosition()
    {
        synchronized (getRawPage())
        {
            return getRawPage().getPosition() + offset;
        }
    }

    public void seek(long position)
    {
        synchronized (getRawPage())
        {
            this.offset = (int) (position - getRawPage().getPosition());
        }
    }

    private Operation newOperation(short type)
    {
        switch (type)
        {
            case Pack.ADD_VACUUM:
                return new Vacuum();
            case Pack.VACUUM:
                return new VacuumCheckpoint();
            case Pack.ADD_MOVE: 
                return new AddMove();
            case Pack.SHIFT_MOVE:
                return new ShiftMove();
            case Pack.CREATE_ADDRESS_PAGE:
                return new CreateAddressPage();
            case Pack.WRITE:
                return new Write();
            case Pack.FREE:
                return new Free();
            case Pack.NEXT_PAGE:
                return new NextOperation();
            case Pack.COPY:
                return new Copy();
            case Pack.TERMINATE:
                return new Terminate();
            case Pack.TEMPORARY:
                return new Temporary();
        }
        throw new IllegalStateException("Invalid type: " + type);
    }
    
    public Operation next()
    {
        ByteBuffer bytes = getByteBuffer();

        Operation operation = newOperation(bytes.getShort());
        operation.read(bytes);
        
        offset = bytes.position();
        
        return operation;
    }
}