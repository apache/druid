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

package org.apache.druid.server.lookup.namespace;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.query.lookup.namespace.JdbcExtractionNamespace;
import org.apache.druid.server.initialization.JdbcAccessSecurityConfig;
import org.apache.druid.server.lookup.namespace.cache.CacheScheduler;
import org.apache.druid.server.lookup.namespace.cache.NamespaceExtractionCacheManager;
import org.apache.druid.server.lookup.namespace.cache.OnHeapNamespaceExtractionCacheManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;

public class JdbcCacheGeneratorTest
{
  private static final MetadataStorageConnectorConfig MISSING_METADATA_STORAGE_CONNECTOR_CONFIG =
      createMetadataStorageConnectorConfig("mydb");

  private static final CacheScheduler.EntryImpl<JdbcExtractionNamespace> KEY =
      EasyMock.mock(CacheScheduler.EntryImpl.class);

  private static final ServiceEmitter SERVICE_EMITTER = new NoopServiceEmitter();

  private static final NamespaceExtractionCacheManager CACHE_MANAGER = new OnHeapNamespaceExtractionCacheManager(
      new Lifecycle(),
      SERVICE_EMITTER,
      new NamespaceExtractionConfig()
  );

  private static final CacheScheduler SCHEDULER = new CacheScheduler(
      SERVICE_EMITTER,
      Collections.emptyMap(),
      CACHE_MANAGER
  );

  private static final String LAST_VERSION = "1";

  private static final String MISSING_JDB_DRIVER_JAR_MSG =
      "JDBC driver JAR files missing from extensions/druid-lookups-cached-global directory";

  private JdbcCacheGenerator target;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Before
  public void setup()
  {
    target = new JdbcCacheGenerator();
  }

  @Test
  public void indicatesMissingJdbcJarsWithTsColumn()
  {
    String tsColumn = "tsColumn";
    JdbcExtractionNamespace missingJarNamespace = createJdbcExtractionNamespace(
        MISSING_METADATA_STORAGE_CONNECTOR_CONFIG,
        tsColumn
    );

    exception.expect(IllegalStateException.class);
    exception.expectMessage(MISSING_JDB_DRIVER_JAR_MSG);

    target.generateCache(missingJarNamespace, KEY, LAST_VERSION, CACHE_MANAGER.allocateCache());
  }

  @Test
  public void indicatesMissingJdbcJarsWithoutTsColumn()
  {
    String missingTsColumn = null;
    @SuppressWarnings("ConstantConditions")  // for missingTsColumn
        JdbcExtractionNamespace missingJarNamespace = createJdbcExtractionNamespace(
        MISSING_METADATA_STORAGE_CONNECTOR_CONFIG,
        missingTsColumn
    );

    exception.expect(IllegalStateException.class);
    exception.expectMessage(MISSING_JDB_DRIVER_JAR_MSG);

    target.generateCache(missingJarNamespace, KEY, LAST_VERSION, CACHE_MANAGER.allocateCache());
  }

  @SuppressWarnings("SameParameterValue")
  private static MetadataStorageConnectorConfig createMetadataStorageConnectorConfig(String type)
  {
    String json = "{\"connectURI\":\"jdbc:" + type + "://localhost:5432\"}";
    try {
      return new ObjectMapper().readValue(json, MetadataStorageConnectorConfig.class);
    }
    catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @SuppressWarnings("SameParameterValue")
  private static JdbcExtractionNamespace createJdbcExtractionNamespace(
      MetadataStorageConnectorConfig metadataStorageConnectorConfig,
      @Nullable String tsColumn
  )
  {
    return new JdbcExtractionNamespace(
        metadataStorageConnectorConfig,
        "table",
        "keyColumn",
        "valueColumn",
        tsColumn,
        "filter",
        Period.ZERO,
        null,
        0,
        null,
        new JdbcAccessSecurityConfig()
    );
  }
}
