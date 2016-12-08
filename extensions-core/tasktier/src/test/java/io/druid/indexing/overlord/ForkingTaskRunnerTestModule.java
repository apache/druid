/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import io.druid.guice.LazySingleton;
import io.druid.guice.PolyBind;
import io.druid.initialization.DruidModule;
import io.druid.metadata.MetadataStorageConnector;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.derby.DerbyConnector;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.skife.jdbi.v2.DBI;

import java.util.List;

public class ForkingTaskRunnerTestModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    final SimpleModule module = new SimpleModule("ForkingTaskRunnerTestModule");
    module.registerSubtypes(BusyTask.class);
    return ImmutableList.of(module);
  }

  public static final String DB_TYPE = "test_derby_db";

  @Override
  public void configure(Binder binder)
  {
    PolyBind.optionBinder(binder, Key.get(MetadataStorageConnector.class))
            .addBinding(DB_TYPE)
            .to(EmbeddedTestDerbyConnector.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(SQLMetadataConnector.class))
            .addBinding(DB_TYPE)
            .to(EmbeddedTestDerbyConnector.class)
            .in(LazySingleton.class);
  }
}

class EmbeddedTestDerbyConnector extends DerbyConnector
{

  @Inject
  public EmbeddedTestDerbyConnector(
      Supplier<MetadataStorageConnectorConfig> config,
      Supplier<MetadataStorageTablesConfig> dbTables
  )
  {
    super(config, dbTables, buildDBI(config));
  }

  private static DBI buildDBI(Supplier<MetadataStorageConnectorConfig> config)
  {
    final MetadataStorageConnectorConfig connectorConfig = config.get();
    final BasicDataSource basicDataSource = new BasicDataSource();
    basicDataSource.setDriver(new EmbeddedDriver());
    basicDataSource.setUrl(connectorConfig.getConnectURI() + ";create=true");
    basicDataSource.setValidationQuery("VALUES 1");
    basicDataSource.setTestOnBorrow(true);
    return new DBI(basicDataSource);
  }
}
