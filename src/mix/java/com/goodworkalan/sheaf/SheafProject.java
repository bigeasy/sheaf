package com.goodworkalan.sheaf.mix;

import com.goodworkalan.go.go.Artifact;
import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.builder.JavaProject;

public class SheafProject extends ProjectModule {
    @Override
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces(new Artifact("com.goodworkalan", "sheaf", "0.1"))
                .main()
                    .depends()
                        .artifact(new Artifact("com.goodworkalan/region/0.1"))
                        .end()
                    .end()
                .test()
                    .depends()
                        .artifact(new Artifact("org.mockito/mockito-core/1.6"))
                        .artifact(new Artifact("org.testng/testng/5.10"))
                        .end()
                    .end()
                .end()
            .end();
    }
}
