package com.goodworkalan.sheaf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.mockito.Mockito.*;

import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SegmentTest
{
    @Test(expectedExceptions=SheafException.class)
    public void ioException() throws IOException
    {
        FileChannel fileChannel = mock(FileChannel.class);
        when(fileChannel.write(Mockito.<ByteBuffer>anyObject(), anyLong())).thenThrow(new IOException());
        new Segment(ByteBuffer.allocate(1), 1, new Object()).write(fileChannel);
    }
}
