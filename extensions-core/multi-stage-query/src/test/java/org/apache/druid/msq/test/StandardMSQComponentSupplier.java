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

package org.apache.druid.msq.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.exec.TestMSQSqlModule;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.msq.test.CalciteMSQTestsHelper.MSQTestModule;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplierDelegate;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;

public class StandardMSQComponentSupplier extends StandardComponentSupplier
{
  /**
   * Upgrade an existing QueryComponentSupplier to support MSQ tests.
   */
  public static class AbstractMSQComponentSupplierDelegate extends QueryComponentSupplierDelegate {

    public AbstractMSQComponentSupplierDelegate(QueryComponentSupplier delegate)
    {
      super(delegate);
    }

    public DruidModule getCoreModule()
    {
      return DruidModuleCollection.of(
            super.getCoreModule(),
            new MSQTestModule(),
            new IndexingServiceTuningConfigModule(),
            new JoinableFactoryModule(),
            new MSQExternalDataSourceModule(),
            new MSQIndexingModule(),
            new TestMSQSqlModule()
          );
    }

    @Override
    public SqlEngine createEngine(
        QueryLifecycleFactory qlf,
        ObjectMapper queryJsonMapper,
        Injector injector)
    {
      return injector.getInstance(MSQTaskSqlEngine.class);
    }

    @Override
    public Boolean isExplainSupported()
    {
      return false;
    }
  }

  public StandardMSQComponentSupplier(TempDirProducer tempFolderProducer)
  {
    super(tempFolderProducer);
  }

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModules(
        CalciteMSQTestsHelper.fetchModules(tempDirProducer::newTempFolder, TestGroupByBuffers.createDefault())
            .toArray(new Module[0])
    );
    builder.addModule(new TestMSQSqlModule());
  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf,
      ObjectMapper queryJsonMapper,
      Injector injector)
  {
    return injector.getInstance(MSQTaskSqlEngine.class);
  }
}
