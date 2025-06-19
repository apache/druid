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

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;

import java.util.Properties;

/**
 * Binds the following metadata configs for all services:
 * <ul>
 * <li>{@link MetadataStorageTablesConfig}</li>
 * <li>{@link MetadataStorageConnectorConfig}</li>
 * <li>{@link CentralizedDatasourceSchemaConfig}</li>
 * </ul>
 * Ideally, the storage configs should be bound only on Coordinator and Overlord,
 * but they are needed for other services too since metadata storage extensions
 * are currently loaded on all services.
 */
public class MetadataConfigModule implements Module
{
  public static final String CENTRALIZED_DATASOURCE_SCHEMA_ENABLED =
      CentralizedDatasourceSchemaConfig.PROPERTY_PREFIX + ".enabled";

  @Override
  public void configure(Binder binder)
  {
    System.out.println("Kashif: Metadata config init done");
    JsonConfigProvider.bind(binder, MetadataStorageTablesConfig.PROPERTY_BASE, MetadataStorageTablesConfig.class);
    JsonConfigProvider.bind(binder, MetadataStorageConnectorConfig.PROPERTY_BASE, MetadataStorageConnectorConfig.class);

    JsonConfigProvider.bind(
        binder,
        CentralizedDatasourceSchemaConfig.PROPERTY_PREFIX,
        CentralizedDatasourceSchemaConfig.class
    );
  }

  public static boolean isSegmentSchemaCacheEnabled(Properties properties)
  {
    return Boolean.parseBoolean(properties.getProperty(CENTRALIZED_DATASOURCE_SCHEMA_ENABLED));
  }
}
