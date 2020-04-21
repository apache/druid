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


package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;


public class SqlSegmentsMetadataManagerProvider implements SegmentsMetadataManagerProvider
{
  private final ObjectMapper jsonMapper;
  private final Supplier<SegmentsMetadataManagerConfig> config;
  private final Supplier<MetadataStorageTablesConfig> storageConfig;
  private final SQLMetadataConnector connector;
  private final Lifecycle lifecycle;

  @Inject
  public SqlSegmentsMetadataManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<SegmentsMetadataManagerConfig> config,
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
  public SegmentsMetadataManager get()
  {
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start()
          {
            connector.createSegmentTable();
          }

          @Override
          public void stop()
          {

          }
        }
    );

    return new SqlSegmentsMetadataManager(
        jsonMapper,
        config,
        storageConfig,
        connector
    );
  }
}
