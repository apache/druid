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

package org.apache.druid.catalog.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;

import javax.inject.Inject;

public class MetastoreManagerImpl implements MetastoreManager
{
  private final ObjectMapper jsonMapper;
  private final MetadataStorageConnector connector;
  private final MetadataStorageConnectorConfig config;
  private final MetadataStorageTablesConfig tablesConfig;

  @Inject
  public MetastoreManagerImpl(
      @Json ObjectMapper jsonMapper,
      MetadataStorageConnector connector,
      Supplier<MetadataStorageConnectorConfig> configSupplier,
      Supplier<MetadataStorageTablesConfig> tablesConfigSupplier
  )
  {
    this.jsonMapper = jsonMapper;
    this.connector = connector;
    this.config = configSupplier.get();
    this.tablesConfig = tablesConfigSupplier.get();
  }

  @Override
  public MetadataStorageConnector connector()
  {
    return connector;
  }

  @Override
  public MetadataStorageConnectorConfig config()
  {
    return config;
  }

  @Override
  public MetadataStorageTablesConfig tablesConfig()
  {
    return tablesConfig;
  }

  @Override
  public boolean createTables()
  {
    return config.isCreateTables();
  }

  @Override
  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  @Override
  public boolean isSql()
  {
    return connector instanceof SQLMetadataConnector;
  }

  @Override
  public SQLMetadataConnector sqlConnector()
  {
    return (SQLMetadataConnector) connector;
  }
}
