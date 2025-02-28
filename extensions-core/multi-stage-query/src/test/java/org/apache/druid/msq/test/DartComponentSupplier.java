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
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;

public class DartComponentSupplier extends AbstractMSQComponentSupplierDelegate
{
  public DartComponentSupplier(TempDirProducer tempFolderProducer)
  {
    super(new StandardComponentSupplier(tempFolderProducer));
  }

  @Override
  public DruidModule getOverrideModule()
  {
    return DruidModuleCollection.of(super.getOverrideModule(),
          new LocalDartModule()
        );
  }

  @Override
  public DruidModule getCoreModule()
  {
    return DruidModuleCollection.of(super.getCoreModule(),
          new DartControllerModule(),
          new DartWorkerModule()
        );
  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf,
      ObjectMapper queryJsonMapper,
      Injector injector)
  {
    return injector.getInstance(DartSqlEngine.class);
  }


  static class LocalDartModule implements DruidModule{

    @Override
    public void configure(Binder binder)
    {
    }

//    @Provides
//    @LazySingleton
//    public MSQTaskSqlEngine createEngine(
//        ObjectMapper queryJsonMapper,
//        MSQTestOverlordServiceClient2 indexingServiceClient)
//    {
//      return new DartSqlEngine(indexingServiceClient, queryJsonMapper, new SegmentGenerationTerminalStageSpecFactory());
//    }
//
//    @Provides
//    @LazySingleton
//    public DartSqlEngine makeSqlEngine(
//        DartControllerContextFactory controllerContextFactory,
//        DartControllerRegistry controllerRegistry,
//        DartControllerConfig controllerConfig
//    )
//    {
//      return new DartSqlEngine(
//          controllerContextFactory,
//          controllerRegistry,
//          controllerConfig,
//          Execs.multiThreaded(controllerConfig.getConcurrentQueries(), "dart-controller-%s")
//      );
//    }
    @Provides
    @LazySingleton
    private MSQTestOverlordServiceClient2 makeOverlordServiceClient(
        ObjectMapper queryJsonMapper,
        Injector injector,
        WorkerMemoryParameters workerMemoryParameters)
    {
      final MSQTestOverlordServiceClient2 indexingServiceClient = new MSQTestOverlordServiceClient2(
          queryJsonMapper,
          injector,
          new MSQTestTaskActionClient(queryJsonMapper, injector),
          workerMemoryParameters,
          ImmutableList.of()
      );
      return indexingServiceClient;
    }


  }
}
