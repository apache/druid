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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.NoopDataSegmentPusher;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;

public class UnifiedIndexerAppenderatorsManagerTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private final UnifiedIndexerAppenderatorsManager manager = new UnifiedIndexerAppenderatorsManager(
      Execs.directExecutor(),
      NoopJoinableFactory.INSTANCE,
      new WorkerConfig(),
      MapCache.create(10),
      new CacheConfig(),
      new CachePopulatorStats(),
      TestHelper.makeJsonMapper(),
      new NoopServiceEmitter(),
      () -> new DefaultQueryRunnerFactoryConglomerate(ImmutableMap.of())
  );

  private final Appenderator appenderator = manager.createOfflineAppenderatorForTask(
      "taskId",
      new DataSchema(
          "myDataSource",
          new TimestampSpec("__time", "millis", null),
          null,
          null,
          new UniformGranularitySpec(Granularities.HOUR, Granularities.HOUR, false, Collections.emptyList()),
          null
      ),
      EasyMock.createMock(AppenderatorConfig.class),
      new FireDepartmentMetrics(),
      new NoopDataSegmentPusher(),
      TestHelper.makeJsonMapper(),
      TestHelper.getTestIndexIO(),
      TestHelper.getTestIndexMergerV9(OnHeapMemorySegmentWriteOutMediumFactory.instance()),
      new NoopRowIngestionMeters(),
      new ParseExceptionHandler(new NoopRowIngestionMeters(), false, 0, 0)
  );

  @Test
  public void test_getBundle_knownDataSource()
  {
    final UnifiedIndexerAppenderatorsManager.DatasourceBundle bundle = manager.getBundle(
        Druids.newScanQueryBuilder()
              .dataSource(appenderator.getDataSource())
              .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
              .build()
    );

    Assert.assertEquals("myDataSource", bundle.getWalker().getDataSource());
  }

  @Test
  public void test_getBundle_unknownDataSource()
  {
    final ScanQuery query = Druids.newScanQueryBuilder()
                                  .dataSource("unknown")
                                  .intervals(new MultipleIntervalSegmentSpec(Intervals.ONLY_ETERNITY))
                                  .build();

    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Could not find segment walker for datasource");

    manager.getBundle(query);
  }
}
