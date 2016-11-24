/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.TestDerbyConnector;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.LookupDimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.JDBCExtractionNamespace;
import io.druid.query.lookup.namespace.KeyValueMap;
import io.druid.query.lookup.namespace.URIExtractionNamespace;
import io.druid.server.lookup.namespace.NamespaceExtractionModule;
import org.apache.commons.lang.StringUtils;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.skife.jdbi.v2.Handle;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class NamespaceLookupReferenceManagerTest
{
  LookupReferencesManager lookupReferencesManager;
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  Injector injector;
  ObjectMapper mapper;

  @Before
  public void setUp() throws IOException
  {
    System.setProperty("druid.extensions.searchCurrentClassloader", "false");

    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjectorWithModules(
            ImmutableList.<Module>of()
        ),
        ImmutableList.of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bindConstant().annotatedWith(Names.named("serviceName")).to("test");
                binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
              }
            },
            new NamespaceExtractionModule()
        )
    );
    mapper = injector.getInstance(ObjectMapper.class);

    lookupReferencesManager = new LookupReferencesManager(new LookupConfig(Files.createTempDir().getAbsolutePath()), mapper);
    Assert.assertTrue("must be closed before start call", lookupReferencesManager.isClosed());
    lookupReferencesManager.start();
    Assert.assertFalse("must start after start call", lookupReferencesManager.isClosed());

  }

  @After
  public void tearDown()
  {
    lookupReferencesManager.stop();
    Assert.assertTrue("stop call should close it", lookupReferencesManager.isClosed());
    System.setProperty("druid.extensions.searchCurrentClassloader", "true");
  }

  @Test
  public void registerUriNamespaceAndLookupTest() throws IOException
  {
    final File tmpFile = temporaryFolder.newFile();
    try (OutputStreamWriter out = new FileWriter(tmpFile)) {
      out.write(StringUtils.join(new String[] {"foo", "bar", "baz"}, ','));
      out.write("\n");
      out.write(StringUtils.join(new String[] {"work", "bad", "good"}, ','));
    }
    ExtractionNamespace uriExtractionNamespace = new URIExtractionNamespace(
        tmpFile.toURI(),
        null, null,
        new URIExtractionNamespace.CSVFlatDataParser(
            ImmutableList.of("key", "val1", "val2"),
            ImmutableList.of (
                new KeyValueMap(KeyValueMap.DEFAULT_MAPNAME, "key", "val1"),
                new KeyValueMap("another", "key", "val2")
            )
        ),
        new Period(0),
        null
    );
    String uriNamespaceJson = mapper.writeValueAsString(uriExtractionNamespace);
    String lookupExtractorFactoryJson = "{\"type\":\"cachedNamespace\",\"extractionNamespace\":"+ uriNamespaceJson +", \"firstCacheTimeout\":1000, \"injective\":false}";
    NamespaceLookupExtractorFactory lookupExtractorFactory =
        mapper.readValue(lookupExtractorFactoryJson, NamespaceLookupExtractorFactory.class);
    Assert.assertEquals(uriExtractionNamespace, lookupExtractorFactory.getExtractionNamespace());

    lookupReferencesManager.put("test", lookupExtractorFactory);
    LookupExtractor lookupExtractor1 = lookupReferencesManager.get("test").get(KeyValueMap.DEFAULT_MAPNAME);
    Assert.assertEquals("bar", lookupExtractor1.apply("foo"));
    Assert.assertEquals("bad", lookupExtractor1.apply("work"));
    LookupExtractor lookupExtractor2 = lookupReferencesManager.get("test").get("another");
    Assert.assertEquals("baz", lookupExtractor2.apply("foo"));
    Assert.assertEquals("good", lookupExtractor2.apply("work"));
    // when no map name is specified, KeyValueMap.DEFAULT_MAPNAME is used as default map name
    // however, this usage is not recommended
    LookupExtractor lookupExtractor3 = lookupReferencesManager.get("test").get();
    Assert.assertEquals(lookupExtractor1, lookupExtractor3);

    DimensionSpec dimensionSpec = new LookupDimensionSpec(
        "dim",
        "out",
        null,
        false,
        null,
        "test",
        "another",
        lookupReferencesManager,
        false
    );

    ExtractionFn fn = dimensionSpec.getExtractionFn();
    Assert.assertEquals("baz", fn.apply("foo"));
    Assert.assertEquals("good", fn.apply("work"));

    DimensionSpec defaultDimSpec = new LookupDimensionSpec(
        "dim",
        "out",
        null,
        false,
        null,
        "test",
        null,
        lookupReferencesManager,
        false
    );

    ExtractionFn fn2 = defaultDimSpec.getExtractionFn();
    Assert.assertEquals("bar", fn2.apply("foo"));
    Assert.assertEquals("bad", fn2.apply("work"));
  }

  @Test
  public void registerJDBCNamespaceAndLookupTest() throws IOException
  {
    String name = "jdbcTest";

    createTable(name);

    MetadataStorageConnectorConfig connectorConfig = mapper.readValue(
        String.format(
            "{\"connectURI\":\"%s\"}",
            derbyConnectorRule.getMetadataConnectorConfig().getConnectURI()
        ),
        MetadataStorageConnectorConfig.class
    );
    ExtractionNamespace jdbcExtractionNamespace = new JDBCExtractionNamespace(
        connectorConfig,
        name,
        null,
        null,
        ImmutableList.of(
            new KeyValueMap(KeyValueMap.DEFAULT_MAPNAME, "lookupKey", "val1"),
            new KeyValueMap("another", "lookupKey", "val2")
        ),
        null,
        new Period(0)
    );
    String jdbcNamespaceJson = mapper.writeValueAsString(jdbcExtractionNamespace);
    String lookupExtractorFactoryJson = "{\"type\":\"cachedNamespace\",\"extractionNamespace\":"+ jdbcNamespaceJson +", \"firstCacheTimeout\":100000, \"injective\":false}";
    NamespaceLookupExtractorFactory lookupExtractorFactory =
        mapper.readValue(lookupExtractorFactoryJson, NamespaceLookupExtractorFactory.class);

    lookupReferencesManager.put(name, lookupExtractorFactory);
    LookupExtractor lookupExtractor1 = lookupReferencesManager.get(name).get(KeyValueMap.DEFAULT_MAPNAME);
    Assert.assertEquals("bar", lookupExtractor1.apply("foo"));
    Assert.assertEquals("bad", lookupExtractor1.apply("work"));
    LookupExtractor lookupExtractor2 = lookupReferencesManager.get(name).get("another");
    Assert.assertEquals("baz", lookupExtractor2.apply("foo"));
    Assert.assertEquals("good", lookupExtractor2.apply("work"));
    // when no map name is specified, KeyValueMap.DEFAULT_MAPNAME is used as default map name
    // however, this usage is not recommended
    LookupExtractor lookupExtractor3 = lookupReferencesManager.get(name).get();
    Assert.assertEquals(lookupExtractor1, lookupExtractor3);

    DimensionSpec dimensionSpec = new LookupDimensionSpec(
        "dim",
        "out",
        null,
        false,
        null,
        name,
        "another",
        lookupReferencesManager,
        false
    );

    ExtractionFn fn = dimensionSpec.getExtractionFn();
    Assert.assertEquals("baz", fn.apply("foo"));
    Assert.assertEquals("good", fn.apply("work"));

    DimensionSpec defaultDimSpec = new LookupDimensionSpec(
        "dim",
        "out",
        null,
        false,
        null,
        name,
        null,
        lookupReferencesManager,
        false
    );

    ExtractionFn fn2 = defaultDimSpec.getExtractionFn();
    Assert.assertEquals("bar", fn2.apply("foo"));
    Assert.assertEquals("bad", fn2.apply("work"));

    dropTable(name);
  }

  private void createTable(String name)
  {
    Handle handle = derbyConnectorRule.getConnector().getDBI().open();
    Assert.assertEquals(
        0,
        handle.createStatement(
            String.format("CREATE TABLE \"%s\" (\"lookupKey\" VARCHAR(10), \"val1\" VARCHAR(10), \"val2\" VARCHAR(10))", name)
        ).setQueryTimeout(1).execute()
    );
    handle.commit();
    handle.createStatement(String.format("TRUNCATE TABLE \"%s\"", name)).setQueryTimeout(1).execute();
    handle.createStatement(String.format("INSERT INTO \"%s\" (\"lookupKey\", \"val1\", \"val2\") VALUES ('foo', 'bar', 'baz')", name)).setQueryTimeout(1).execute();
    handle.createStatement(String.format("INSERT INTO \"%s\" (\"lookupKey\", \"val1\", \"val2\") VALUES ('work', 'bad', 'good')", name)).setQueryTimeout(1).execute();
    handle.close();
  }

  private void dropTable(String name)
  {
    Handle handle = derbyConnectorRule.getConnector().getDBI().open();
    handle.createStatement(String.format("DROP TABLE \"%s\"", name)).setQueryTimeout(1).execute();
    handle.close();
  }
}
