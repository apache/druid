/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.LocalInputSourceFactory;
import org.apache.druid.iceberg.common.IcebergDruidModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Validates JSON round-trip of {@link IcebergFileTaskInputSource} and propagation of
 * {@code hadoopConfigOverrides} from {@link HiveIcebergCatalog} through {@link IcebergCatalog.FileScanResult}.
 *
 * Regression net for the v2 split deserialization issue where {@code @JacksonInject @HiveConf Configuration}
 * binding is required on the worker side.
 */
public class IcebergFileTaskInputSourceSerdeTest
{
  private ObjectMapper mapper;
  private Configuration injectedConf;

  @Before
  public void setUp()
  {
    mapper = new DefaultObjectMapper();
    for (final Module module : new IcebergDruidModule().getJacksonModules()) {
      mapper.registerModule(module);
    }
    injectedConf = new Configuration(false);
    injectedConf.set("fs.s3a.endpoint", "https://injected.example.com");
    mapper.setInjectableValues(
        new InjectableValues.Std().addValue(Configuration.class, injectedConf)
    );
  }

  @Test
  public void testJsonRoundTripPreservesAllSerializableFields() throws Exception
  {
    final Map<String, String> fileIOProperties = new LinkedHashMap<>();
    fileIOProperties.put("io.k1", "v1");
    final Map<String, String> hadoopOverrides = new LinkedHashMap<>();
    hadoopOverrides.put("fs.s3a.access.key", "AKIA");
    hadoopOverrides.put("fs.s3a.secret.key", "SECRET");

    final IcebergFileTaskInputSource original = new IcebergFileTaskInputSource(
        "/warehouse/db/t/data/file.parquet",
        Collections.emptyList(),
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
        new LocalInputSourceFactory(),
        "org.apache.iceberg.hadoop.HadoopFileIO",
        fileIOProperties,
        hadoopOverrides,
        "PARQUET",
        injectedConf
    );

    final String json = mapper.writeValueAsString(original);
    final IcebergFileTaskInputSource round = mapper.readValue(json, IcebergFileTaskInputSource.class);

    Assert.assertEquals(original.getDataFilePath(), round.getDataFilePath());
    Assert.assertEquals(original.getTableSchemaJson(), round.getTableSchemaJson());
    Assert.assertEquals(original.getFileIOImpl(), round.getFileIOImpl());
    Assert.assertEquals(original.getFileIOProperties(), round.getFileIOProperties());
    Assert.assertEquals(original.getHadoopConfigOverrides(), round.getHadoopConfigOverrides());
    Assert.assertEquals(original.getFileFormat(), round.getFileFormat());
  }

  @Test
  public void testRoundTripDoesNotSerializeHadoopConfiguration() throws Exception
  {
    final IcebergFileTaskInputSource original = new IcebergFileTaskInputSource(
        "/p/file.parquet",
        Collections.emptyList(),
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
        new LocalInputSourceFactory(),
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        "PARQUET",
        injectedConf
    );

    final String json = mapper.writeValueAsString(original);
    Assert.assertFalse(
        "Hadoop Configuration object must not appear in serialized JSON",
        json.contains("baseHadoopConf")
    );
  }

  @Test
  public void testDeserializationWithoutInjectedConfFallsBackSafely() throws Exception
  {
    final ObjectMapper noInjectMapper = new DefaultObjectMapper();
    for (final Module module : new IcebergDruidModule().getJacksonModules()) {
      noInjectMapper.registerModule(module);
    }
    noInjectMapper.setInjectableValues(
        new InjectableValues.Std().addValue(Configuration.class, null)
    );

    final IcebergFileTaskInputSource original = new IcebergFileTaskInputSource(
        "/p/file.parquet",
        Collections.emptyList(),
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
        new LocalInputSourceFactory(),
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        "PARQUET",
        null
    );

    final String json = mapper.writeValueAsString(original);
    final IcebergFileTaskInputSource round = noInjectMapper.readValue(json, IcebergFileTaskInputSource.class);
    Assert.assertNotNull(round);
    Assert.assertEquals("/p/file.parquet", round.getDataFilePath());
  }

  @Test
  public void testJacksonSubtypeRegistration() throws Exception
  {
    final IcebergFileTaskInputSource original = new IcebergFileTaskInputSource(
        "/p/file.parquet",
        Collections.emptyList(),
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
        new LocalInputSourceFactory(),
        null,
        Collections.emptyMap(),
        Collections.emptyMap(),
        "PARQUET",
        injectedConf
    );

    final String json = mapper.writeValueAsString(original);
    final InputSource round = mapper.readValue(json, InputSource.class);
    Assert.assertTrue(round instanceof IcebergFileTaskInputSource);
  }

  @Test
  public void testNullOverridesDefaultToEmpty()
  {
    final IcebergFileTaskInputSource src = new IcebergFileTaskInputSource(
        "/p/file.parquet",
        Collections.emptyList(),
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]}",
        new LocalInputSourceFactory(),
        null,
        null,
        null,
        null,
        injectedConf
    );
    Assert.assertNotNull(src.getFileIOProperties());
    Assert.assertNotNull(src.getHadoopConfigOverrides());
    Assert.assertTrue(src.getFileIOProperties().isEmpty());
    Assert.assertTrue(src.getHadoopConfigOverrides().isEmpty());
    Assert.assertEquals("PARQUET", src.getFileFormat());
  }

  @Test
  public void testHiveCatalogExposesHadoopConfigOverrides()
  {
    final Configuration catalogConf = new Configuration(false);
    catalogConf.set("fs.s3a.endpoint", "https://catalog.example.com");
    catalogConf.set("hive.metastore.uris", "thrift://meta:9083");

    final HiveIcebergCatalog catalog = new HiveIcebergCatalog(
        "/tmp/warehouse",
        "thrift://meta:9083",
        new HashMap<>(),
        false,
        mapper,
        catalogConf
    );

    final Map<String, String> overrides = catalog.getHadoopConfigOverrides();
    Assert.assertEquals(
        "https://catalog.example.com",
        overrides.get("fs.s3a.endpoint")
    );
    Assert.assertEquals(
        "thrift://meta:9083",
        overrides.get("hive.metastore.uris")
    );
  }

  @Test
  public void testBaseCatalogReturnsEmptyOverrides()
  {
    final java.io.File warehouseDir = org.apache.druid.java.util.common.FileUtils.createTempDir();
    final LocalCatalog local = new LocalCatalog(
        warehouseDir.getPath(),
        new HashMap<>(),
        true
    );
    Assert.assertEquals(Collections.emptyMap(), local.getHadoopConfigOverrides());
  }
}
