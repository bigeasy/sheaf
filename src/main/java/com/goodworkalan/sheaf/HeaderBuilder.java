package com.goodworkalan.sheaf;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class HeaderBuilder<K>
{
    private int offset;
    
    private final Map<K, List<Integer>> offsets;
    
    public HeaderBuilder()
    {
        this.offsets = new HashMap<K, List<Integer>>();
    }
    
    public void addField(K key, int length)
    {
        List<Integer> mapping = new ArrayList<Integer>(2);
        mapping.add(offset);
        mapping.add(length);
        offsets.put(key, mapping);
        offset += length;
    }
    
    public Header<K> newHeader(long position, ByteBuffer byteBuffer)
    {
        if (byteBuffer.capacity() < offset)
        {
            throw new IllegalArgumentException();
        }
        return new Header<K>(position, offsets, byteBuffer, new ReentrantLock());
    }
    
    public Header<K> newHeader(long position)
    {
        ByteBuffer byteBuffer = ByteBuffer.allocate(offset);
        return newHeader(position, byteBuffer);
    }
}
