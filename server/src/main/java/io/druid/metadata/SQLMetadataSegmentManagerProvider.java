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
import com.google.common.base.Supplier;
import com.google.inject.Inject;

import io.druid.java.util.common.lifecycle.Lifecycle;


public class SQLMetadataSegmentManagerProvider implements MetadataSegmentManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final Supplier<MetadataSegmentManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> storageConfig;
  private final SQLMetadataConnector connector;
  private final Lifecycle lifecycle;

  @Inject
  public SQLMetadataSegmentManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<MetadataSegmentManagerConfig> config,
      Supplier<MetadataStorageTablesConfig> storageConfig,
      SQLMetadataConnector connector,
      Lifecycle lifecycle
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.storageConfig = storageConfig;
    this.connector = connector;
    this.lifecycle = lifecycle;
  }

  @Override
  public MetadataSegmentManager get()
  {
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            connector.createSegmentTable();
          }

          @Override
          public void stop()
          {

          }
        }
    );

    return new SQLMetadataSegmentManager(
        jsonMapper,
        config,
        storageConfig,
        connector
    );
  }
}
