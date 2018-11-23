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
import org.junit.Assert;
import org.junit.Test;

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

    Assert.assertEquals(props.getProperty("druid.metadata.storage.tables.base"), config.getBase());
    Assert.assertEquals(props.getProperty("druid.metadata.storage.tables.segments"), config.getSegmentsTable());
    Assert.assertEquals(props.getProperty("druid.metadata.storage.tables.rules"), config.getRulesTable());
    Assert.assertEquals(props.getProperty("druid.metadata.storage.tables.config"), config.getConfigTable());
    Assert.assertEquals(
        props.getProperty("druid.metadata.storage.tables.tasks"),
        config.getEntryTable(MetadataStorageTablesConfig.TASK_ENTRY_TYPE)
    );
    Assert.assertEquals(
        props.getProperty("druid.metadata.storage.tables.taskLog"),
        config.getLogTable(MetadataStorageTablesConfig.TASK_ENTRY_TYPE)
    );
    Assert.assertEquals(
        props.getProperty("druid.metadata.storage.tables.taskLock"),
        config.getLockTable(MetadataStorageTablesConfig.TASK_ENTRY_TYPE)
    );
    Assert.assertEquals(props.getProperty("druid.metadata.storage.tables.dataSource"), config.getDataSourceTable());
    Assert.assertEquals(props.getProperty("druid.metadata.storage.tables.supervisors"), config.getSupervisorTable());
  }
}
