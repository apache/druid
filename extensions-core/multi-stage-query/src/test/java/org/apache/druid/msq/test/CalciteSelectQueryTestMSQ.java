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
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.guice.JoinableFactoryModule;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.guice.MSQExternalDataSourceModule;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.querykit.DataSegmentProvider;
import org.apache.druid.msq.querykit.LazyResourceHolder;
import org.apache.druid.msq.sql.MSQTaskSqlEngine;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.lookup.LookupReferencesManager;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusher;
import org.apache.druid.segment.loading.LocalDataSegmentPusherConfig;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class CalciteSelectQueryTestMSQ extends BaseCalciteQueryTest
{

  @Override
  public void configureGuice(DruidInjectorBuilder builder)
  {
    super.configureGuice(builder);
    builder.addModule(binder -> {
      final LookupReferencesManager lookupReferencesManager =
          EasyMock.createStrictMock(LookupReferencesManager.class);
      EasyMock.expect(lookupReferencesManager.getAllLookupNames()).andReturn(Collections.emptySet()).anyTimes();
      EasyMock.replay(lookupReferencesManager);
      binder.bind(LookupReferencesManager.class).toInstance(lookupReferencesManager);
      binder.bind(AppenderatorsManager.class).toProvider(() -> null);

      // Requirements of JoinableFactoryModule
      binder.bind(SegmentManager.class).toInstance(EasyMock.createMock(SegmentManager.class));

      binder.bind(new TypeLiteral<Set<NodeRole>>()
      {
      }).annotatedWith(Self.class).toInstance(ImmutableSet.of(NodeRole.PEON));

      DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
      {
        @Override
        public String getFormatString()
        {
          return "test";
        }
      };
      binder.bind(DruidProcessingConfig.class).toInstance(druidProcessingConfig);
      binder.bind(QueryProcessingPool.class)
            .toInstance(new ForwardingQueryProcessingPool(Execs.singleThreaded("Test-runner-processing-pool")));

      // Select queries donot require this
      Injector dummyInjector = GuiceInjectors.makeStartupInjectorWithModules(
          ImmutableList.of(
              binder1 -> {
                binder1.bind(ExprMacroTable.class).toInstance(CalciteTests.createExprMacroTable());
                binder1.bind(DataSegment.PruneSpecsHolder.class).toInstance(DataSegment.PruneSpecsHolder.DEFAULT);
              }
          )
      );
      ObjectMapper testMapper = MSQTestBase.setupObjectMapper(dummyInjector);
      IndexIO indexIO = new IndexIO(testMapper, () -> 0);
      SegmentCacheManager segmentCacheManager = null;
      try {
        segmentCacheManager = new SegmentCacheManagerFactory(testMapper).manufacturate(temporaryFolder.newFolder("test"));
      }
      catch (IOException e) {
        e.printStackTrace();
      }
      LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
      MSQTestSegmentManager segmentManager = new MSQTestSegmentManager(segmentCacheManager, indexIO);
      try {
        config.storageDirectory = temporaryFolder.newFolder("localsegments");
      }
      catch (IOException e) {
        throw new ISE(e, "Unable to create folder");
      }
      binder.bind(DataSegmentPusher.class).toProvider(() -> new MSQTestDelegateDataSegmentPusher(
          new LocalDataSegmentPusher(config),
          segmentManager
      ));
      binder.bind(DataSegmentAnnouncer.class).toInstance(new NoopDataSegmentAnnouncer());
      binder.bind(DataSegmentProvider.class)
            .toInstance(new Dumm));
    });

    builder.addModule(new IndexingServiceTuningConfigModule());
    builder.addModule(new JoinableFactoryModule());
    builder.addModule(new MSQExternalDataSourceModule());
    builder.addModule(new MSQIndexingModule());

  }

  @Override
  public SqlEngine createEngine(
      QueryLifecycleFactory qlf, ObjectMapper queryJsonMapper,
      Injector injector
  )
  {
    final WorkerMemoryParameters workerMemoryParameters = Mockito.spy(
        WorkerMemoryParameters.createInstance(
            WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
            WorkerMemoryParameters.PROCESSING_MINIMUM_BYTES * 50,
            2,
            10,
            2
        )
    );
    MSQTestOverlordServiceClient indexingServiceClient = new MSQTestOverlordServiceClient(
        queryJsonMapper,
        injector,
        new MSQTestTaskActionClient(queryJsonMapper),
        workerMemoryParameters
    );
    return new MSQTaskSqlEngine(indexingServiceClient, queryJsonMapper);
  }

  @Test
  public void testOrderThenLimitThenFilter()
  {
    testQueryWithMSQ(
        "SELECT dim1 FROM "
        + "(SELECT __time, dim1 FROM druid.foo ORDER BY __time DESC LIMIT 4) "
        + "WHERE dim1 IN ('abc', 'def')",
        ImmutableList.of(
            newScanQueryBuilder()
                .dataSource(
                    new QueryDataSource(
                        newScanQueryBuilder()
                            .dataSource(CalciteTests.DATASOURCE1)
                            .intervals(querySegmentSpec(Filtration.eternity()))
                            .columns(ImmutableList.of("__time", "dim1"))
                            .limit(4)
                            .order(ScanQuery.Order.DESCENDING)
                            .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                            .context(QUERY_CONTEXT_DEFAULT)
                            .build()
                    )
                )
                .intervals(querySegmentSpec(Filtration.eternity()))
                .columns(ImmutableList.of("dim1"))
                .filters(in("dim1", Arrays.asList("abc", "def"), null))
                .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                .context(QUERY_CONTEXT_DEFAULT)
                .build()
        ),
        ImmutableList.of(
            new Object[]{"abc"},
            new Object[]{"def"}
        )
    );
  }
}
