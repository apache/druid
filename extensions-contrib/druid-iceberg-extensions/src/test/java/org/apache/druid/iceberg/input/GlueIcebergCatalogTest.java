package org.apache.druid.iceberg.input;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;

public class GlueIcebergCatalogTest  {
    private final ObjectMapper mapper = new DefaultObjectMapper();
    public void setUp() throws Exception {
    }

    public void tearDown() throws Exception {
    }

    @Test
    public void testCatalogCreate() {
        GlueIcebergCatalog glueCatalog = new GlueIcebergCatalog(
                new HashMap<>(),
                true,
                mapper
        );
        Assert.assertEquals("glue", glueCatalog.retrieveCatalog().name());
    }
    @Test
    public void testIsCaseSensitive() {
        GlueIcebergCatalog glueCatalog = new GlueIcebergCatalog(
                new HashMap<>(),
                true,
                mapper
        );
        Assert.assertEquals(true, glueCatalog.isCaseSensitive());
    }
}