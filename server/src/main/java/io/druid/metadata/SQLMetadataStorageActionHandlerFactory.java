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

package io.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

public class SQLMetadataStorageActionHandlerFactory implements MetadataStorageActionHandlerFactory
{
  private final SQLMetadataConnector connector;
  private final MetadataStorageTablesConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public SQLMetadataStorageActionHandlerFactory(
      SQLMetadataConnector connector,
      MetadataStorageTablesConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.connector = connector;
    this.config = config;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public <A,B,C,D> MetadataStorageActionHandler<A,B,C,D> create(
      final String entryType,
      MetadataStorageActionHandlerTypes<A,B,C,D> payloadTypes
  )
  {
    return new SQLMetadataStorageActionHandler<>(
        connector,
        jsonMapper,
        payloadTypes,
        entryType,
        config.getEntryTable(entryType),
        config.getLogTable(entryType),
        config.getLockTable(entryType)
    );
  }
}
