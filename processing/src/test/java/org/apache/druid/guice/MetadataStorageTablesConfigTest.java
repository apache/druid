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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Properties;

public class MetadataStorageTablesConfigTest
{
  @Test
  public void testSerdeMetadataStorageTablesConfig()
  {
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.install(new PropertiesModule(Collections.singletonList("test.runtime.properties")));
            binder.install(new ConfigModule());
            binder.install(new DruidGuiceExtensions());
            JsonConfigProvider.bind(binder, "druid.metadata.storage.tables", MetadataStorageTablesConfig.class);
          }

          @Provides
          @LazySingleton
          public ObjectMapper jsonMapper()
          {
            return new DefaultObjectMapper();
          }
        }
    );

    Properties props = injector.getInstance(Properties.class);
    MetadataStorageTablesConfig config = injector.getInstance(MetadataStorageTablesConfig.class);

    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.base"), config.getBase());
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.segments"), config.getSegmentsTable());
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.segmentSchemas"), config.getSegmentSchemasTable());
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.rules"), config.getRulesTable());
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.config"), config.getConfigTable());
    Assertions.assertEquals(
        props.getProperty("druid.metadata.storage.tables.tasks"),
        config.getTasksTable()
    );
    Assertions.assertEquals(
        props.getProperty("druid.metadata.storage.tables.taskLock"),
        config.getTaskLockTable()
    );
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.dataSource"), config.getDataSourceTable());
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.supervisors"), config.getSupervisorTable());
    Assertions.assertEquals(props.getProperty("druid.metadata.storage.tables.upgradeSegments"), config.getUpgradeSegmentsTable());
  }

  @Test
  public void testReadConfig()
  {
    MetadataStorageTablesConfig fromBase = MetadataStorageTablesConfig.fromBase("druid.metadata.storage.tables");
    Assertions.assertEquals("druid.metadata.storage.tables_segments", fromBase.getSegmentsTable());
    Assertions.assertEquals("druid.metadata.storage.tables_segmentSchemas", fromBase.getSegmentSchemasTable());
    Assertions.assertEquals("druid.metadata.storage.tables_tasklocks", fromBase.getTaskLockTable());
    Assertions.assertEquals("druid.metadata.storage.tables_rules", fromBase.getRulesTable());
    Assertions.assertEquals("druid.metadata.storage.tables_config", fromBase.getConfigTable());
    Assertions.assertEquals("druid.metadata.storage.tables_dataSource", fromBase.getDataSourceTable());
    Assertions.assertEquals("druid.metadata.storage.tables_supervisors", fromBase.getSupervisorTable());
    Assertions.assertEquals("druid.metadata.storage.tables_upgradeSegments", fromBase.getUpgradeSegmentsTable());
  }
}
