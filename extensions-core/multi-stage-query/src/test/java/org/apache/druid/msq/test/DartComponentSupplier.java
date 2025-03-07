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
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Merging;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.DartControllerContextFactory;
import org.apache.druid.msq.dart.controller.sql.DartSqlEngine;
import org.apache.druid.msq.dart.guice.DartControllerModule;
import org.apache.druid.msq.dart.guice.DartModules;
import org.apache.druid.msq.dart.guice.DartWorkerMemoryManagementModule;
import org.apache.druid.msq.dart.guice.DartWorkerModule;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.query.TestBufferPool;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.guice.ServiceClientModule;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.sql.avatica.DartDruidMeta;
import org.apache.druid.sql.avatica.DruidMeta;
import org.apache.druid.sql.calcite.TempDirProducer;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.DruidModuleCollection;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class DartComponentSupplier extends AbstractMSQComponentSupplierDelegate
{
  public DartComponentSupplier(TempDirProducer tempFolderProducer)
  {
    super(new StandardComponentSupplier(tempFolderProducer));
  }

  @Override
  public void gatherProperties(Properties properties)
  {
    super.gatherProperties(properties);
    properties.put(DartModules.DART_ENABLED_PROPERTY, "true");
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
          new DartWorkerModule(),
          new DartWorkerMemoryManagementModule(),
          new L1()
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



  static class L1 implements DruidModule{

    @Provides
    @EscalatedGlobal
    final ServiceClientFactory getServiceClientFactory(HttpClient ht) {
      return ServiceClientModule.makeServiceClientFactory(ht);

    }

    @Provides
    final DruidNodeDiscoveryProvider getDiscoveryProvider() {
      return null;
    }


    @Override
    public void configure(Binder binder)
    {
//      binder.bind(new TypeLiteral<BlockingPool<ByteBuffer>>(){})
//      .annotatedWith(Merging.class)
//      .to(TestBufferPool.class);

    }

  }
  static class LocalDartModule implements DruidModule{

    @Provides
    @LazySingleton
    public DruidMeta createMeta(DartDruidMeta druidMeta)
    {
      return druidMeta;
    }

    @Override
    public void configure(Binder binder)
    {
      binder.bind(new TypeLiteral<NonBlockingPool<ByteBuffer>>(){})
      .annotatedWith(Merging.class)
      .to(TestBufferPool.class);

      if(true) {
      binder.bind(DartControllerContextFactory.class)
          .to(TestDartControllerContextFactoryImpl.class)
          .in(LazySingleton.class);
      }else {
        binder.bind(DartControllerContextFactory.class)
        .to(MSQTestControllerContext.class)
        .in(LazySingleton.class);

      }
    }

    @Provides
    @LazySingleton
    @Dart
    Map<String, Worker> workerMap()
    {
      return new HashMap<String, Worker>();
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
  }
}
