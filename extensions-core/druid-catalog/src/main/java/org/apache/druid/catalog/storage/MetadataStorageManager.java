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

/**
 * Represents the metastore manager database and its implementation.
 * Abstracts away the various kick-knacks used to define the metastore.
 * The metastore operations are defined via table-specific classes.
 */
public class MetadataStorageManager
{
  private final ObjectMapper jsonMapper;
  private final MetadataStorageConnector connector;
  private final MetadataStorageConnectorConfig config;
  private final MetadataStorageTablesConfig tablesConfig;

  @Inject
  public MetadataStorageManager(
      @Json final ObjectMapper jsonMapper,
      final MetadataStorageConnector connector,
      final Supplier<MetadataStorageConnectorConfig> configSupplier,
      final Supplier<MetadataStorageTablesConfig> tablesConfigSupplier
  )
  {
    this.jsonMapper = jsonMapper;
    this.connector = connector;
    this.config = configSupplier.get();
    this.tablesConfig = tablesConfigSupplier.get();
  }

  public MetadataStorageConnectorConfig config()
  {
    return config;
  }

  public MetadataStorageTablesConfig tablesConfig()
  {
    return tablesConfig;
  }

  /**
   * Object mapper to use for serializing and deserializing
   * JSON objects stored in the metastore DB.
   */
  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }

  /**
   * Is the implementation SQL-based?
   */
  public boolean isSql()
  {
    return connector instanceof SQLMetadataConnector;
  }

  /**
   * If SQL based, return the SQL version of the metastore
   * connector. Throws an exception if not SQL-based.
   */
  public SQLMetadataConnector sqlConnector()
  {
    return (SQLMetadataConnector) connector;
  }
}
