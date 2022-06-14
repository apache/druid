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

package org.apache.druid.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageConnectorConfig;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;

/**
 * Represents the metastore manager database and its implementation.
 * Abstracts away the various kick-knacks used to define the metastore.
 * The metastore operations are defined via table-specific classes.
 */
public interface MetastoreManager
{
  MetadataStorageConnector connector();
  MetadataStorageConnectorConfig config();
  MetadataStorageTablesConfig tablesConfig();

  /**
   * Whether to create tables if they do not exist.
   */
  boolean createTables();

  /**
   * Object mapper to use for serializing and deserializing
   * JSON objects stored in the metastore DB.
   */
  ObjectMapper jsonMapper();

  /**
   * Is the implementation SQL-based?
   */
  boolean isSql();

  /**
   * If SQL based, return the SQL version of the metastore
   * connector. Throws an exception if not SQL-based.
   */
  SQLMetadataConnector sqlConnector();
}
