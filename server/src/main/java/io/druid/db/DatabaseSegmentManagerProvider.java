/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.metamx.common.lifecycle.Lifecycle;

/**
 */
public class DatabaseSegmentManagerProvider implements Provider<DatabaseSegmentManager>
{
  private final ObjectMapper jsonMapper;
  private final Supplier<DatabaseSegmentManagerConfig> config;
  private final Supplier<DbTablesConfig> dbTables;
  private final DbConnector dbConnector;
  private final Lifecycle lifecycle;

  @Inject
  public DatabaseSegmentManagerProvider(
      ObjectMapper jsonMapper,
      Supplier<DatabaseSegmentManagerConfig> config,
      Supplier<DbTablesConfig> dbTables,
      DbConnector dbConnector,
      Lifecycle lifecycle
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.dbTables = dbTables;
    this.dbConnector = dbConnector;
    this.lifecycle = lifecycle;
  }

  @Override
  public DatabaseSegmentManager get()
  {
    lifecycle.addHandler(
        new Lifecycle.Handler()
        {
          @Override
          public void start() throws Exception
          {
            dbConnector.createSegmentTable();
          }

          @Override
          public void stop()
          {

          }
        }
    );

    return new DatabaseSegmentManager(
        jsonMapper,
        config,
        dbTables,
        dbConnector.getDBI()
    );
  }
}
