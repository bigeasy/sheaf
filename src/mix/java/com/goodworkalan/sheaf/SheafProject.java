package com.goodworkalan.sheaf.mix;

import com.goodworkalan.mix.ProjectModule;
import com.goodworkalan.mix.builder.Builder;
import com.goodworkalan.mix.builder.JavaProject;

/**
 * Builds the project definition for Sheaf.
 *
 * @author Alan Gutierrez
 */
public class SheafProject implements ProjectModule {
    /**
     * Build the project definition for Sheaf.
     *
     * @param builder
     *          The project builder.
     */
    public void build(Builder builder) {
        builder
            .cookbook(JavaProject.class)
                .produces("com.github.bigeasy.sheaf/sheaf/0.1")
                .depends()
                    .production("com.github.bigeasy.region/region/0.+1")
                    .development("org.mockito/mockito-core/1.6")
                    .development("org.testng/testng-jdk15/5.10")
                    .end()
                .end()
            .end();
    }
}
