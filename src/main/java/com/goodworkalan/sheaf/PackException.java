package com.goodworkalan.sheaf;

public final class PackException
extends RuntimeException
{
    private static final long serialVersionUID = 20070821L;
    
    private final int code;

    public PackException(int code)
    {
        super(Integer.toString(code));
        this.code = code;
    }

    public PackException(int code, Throwable cause)
    {
        super(Integer.toString(code), cause);
        this.code = code;
    }
    
    public int getCode()
    {
        return code;
    }
}