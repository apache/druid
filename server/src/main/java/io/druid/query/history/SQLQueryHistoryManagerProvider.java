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

package io.druid.query.history;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;

public class SQLQueryHistoryManagerProvider implements QueryHistoryManagerProvider
{

  private final SQLMetadataConnector connector;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final ObjectMapper jsonMapper;
  private final Lifecycle lifecycle;
  private final QueryHistoryConfig queryHistoryConfig;

  @Inject
  public SQLQueryHistoryManagerProvider(
      SQLMetadataConnector connector,
      Supplier<MetadataStorageTablesConfig> dbTables,
      @Json ObjectMapper jsonMapper,
      Lifecycle lifecycle,
      QueryHistoryConfig queryHistoryConfig
  )
  {
    this.connector = connector;
    this.dbTables = dbTables;
    this.jsonMapper = jsonMapper;
    this.lifecycle = lifecycle;
    this.queryHistoryConfig = queryHistoryConfig;
  }

  @Override
  public QueryHistoryManager get()
  {
    try {
      lifecycle.addMaybeStartHandler(
          new Lifecycle.Handler()
          {
            @Override
            public void start() throws Exception
            {
              connector.createQueryHistoryTable();
            }

            @Override
            public void stop()
            {

            }
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return new SQLQueryHistoryManager(connector, dbTables, jsonMapper, queryHistoryConfig);
  }
}
