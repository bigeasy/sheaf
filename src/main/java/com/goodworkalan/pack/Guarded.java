package com.goodworkalan.pack;

import java.util.List;


interface Guarded<T>
{
    public T run(List<MoveLatch> userMoveLatches);
}