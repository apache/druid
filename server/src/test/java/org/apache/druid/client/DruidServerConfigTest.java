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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class DruidServerConfigTest
{
  private File testSegmentCacheDir1;
  private File testSegmentCacheDir2;

  @TempDir(cleanup = CleanupMode.ALWAYS)
  private File tmpFolder;

  private final ObjectMapper mapper = new DefaultObjectMapper();

  private static final Module SERVER_CONFIG_MODULE = (binder) -> {
    binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
    binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
    binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
  };

  @BeforeEach
  void setUp()
  {
    testSegmentCacheDir1 = new File(tmpFolder, "segment_cache_folder1");
    testSegmentCacheDir2 = new File(tmpFolder, "segment_cache_folder2");
  }

  @Test
  void testBasicInjection()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(), ImmutableList.of(SERVER_CONFIG_MODULE)
    );
    final DruidServerConfig druidServerConfig = injector.getInstance(DruidServerConfig.class);

    assertNotNull(druidServerConfig);
    assertEquals(DruidServerConfig.class, druidServerConfig.getClass());
  }

  @Test
  void testCombinedSize()
  {
    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig1 = new StorageLocationConfig(testSegmentCacheDir1, 10000000000L, null);
    final StorageLocationConfig locationConfig2 = new StorageLocationConfig(testSegmentCacheDir2, 20000000000L, null);
    locations.add(locationConfig1);
    locations.add(locationConfig2);
    DruidServerConfig druidServerConfig = new DruidServerConfig(new SegmentLoaderConfig().withLocations(locations));
    assertEquals(30000000000L, druidServerConfig.getMaxSize());
  }

  @Test
  void testServerMaxSizePrecedence() throws Exception
  {
    String serverConfigWithDefaultSizeStr = "{\"maxSize\":0,\"tier\":\"_default_tier\",\"priority\":0,"
                                            + "\"hiddenProperties\":[\"druid.metadata.storage.connector.password\","
                                            + "\"druid.s3.accessKey\",\"druid.s3.secretKey\"]}\n";

    String serverConfigWithNonDefaultSizeStr = "{\"maxSize\":123456,\"tier\":\"_default_tier\",\"priority\":0,"
                                               + "\"hiddenProperties\":[\"druid.metadata.storage.connector.password\","
                                               + "\"druid.s3.accessKey\",\"druid.s3.secretKey\"]}\n";

    final List<StorageLocationConfig> locations = new ArrayList<>();
    final StorageLocationConfig locationConfig1 = new StorageLocationConfig(testSegmentCacheDir1, 10000000000L, null);
    locations.add(locationConfig1);
    mapper.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, new DefaultObjectMapper())
                                                         .addValue(
                                                             SegmentLoaderConfig.class,
                                                             new SegmentLoaderConfig().withLocations(locations)
                                                         ));

    DruidServerConfig serverConfigWithDefaultSize = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(serverConfigWithDefaultSizeStr, DruidServerConfig.class)
        ),
        DruidServerConfig.class
    );

    DruidServerConfig serverConfigWithNonDefaultSize = mapper.readValue(
        mapper.writeValueAsString(
            mapper.readValue(serverConfigWithNonDefaultSizeStr, DruidServerConfig.class)
        ),
        DruidServerConfig.class
    );

    assertEquals(serverConfigWithDefaultSize.getMaxSize(), 10000000000L);
    assertEquals(serverConfigWithNonDefaultSize.getMaxSize(), 123456L);
  }
}

