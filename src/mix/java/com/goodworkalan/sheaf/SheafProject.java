package com.goodworkalan.sheaf.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.builder.JavaProject;

public class SheafProject extends ProjectModule {
    @Override
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.sheaf/sheaf/0.1")
                .main()
                    .depends()
                        .include("com.goodworkalan/region/0.+1")
                        .end()
                    .end()
                .test()
                    .depends()
                        .include("org.mockito/mockito-core/1.6")
                        .include("org.testng/testng-jdk15/5.10")
                        .end()
                    .end()
                .end()
            .end();
    }
}
