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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.first.LongFirstAggregatorFactory;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class SegmentSchemaBackFillQueueTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule(getEnabledConfig());

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  public void testPublishSchema() throws InterruptedException
  {
    TestDerbyConnector derbyConnector = derbyConnectorRule.getConnector();
    derbyConnector.createSegmentSchemasTable();
    derbyConnector.createSegmentTable();

    SegmentSchemaManager segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        mapper,
        derbyConnector
    );

    SegmentSchemaTestUtils segmentSchemaTestUtils =
        new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);
    SegmentSchemaCache segmentSchemaCache = new SegmentSchemaCache(new NoopServiceEmitter());
    CentralizedDatasourceSchemaConfig config = CentralizedDatasourceSchemaConfig.create();
    config.setEnabled(true);
    config.setBackFillEnabled(true);
    config.setBackFillPeriod(1);

    CountDownLatch latch = new CountDownLatch(1);

    SegmentSchemaBackFillQueue segmentSchemaBackFillQueue =
        new SegmentSchemaBackFillQueue(
            segmentSchemaManager,
            ScheduledExecutors::fixed,
            segmentSchemaCache,
            new FingerprintGenerator(mapper),
            new NoopServiceEmitter(),
            config
        ) {
          @Override
          public void processBatchesDue()
          {
            super.processBatchesDue();
            latch.countDown();
          }
        };

    final DataSegment segment = new DataSegment(
        "foo",
        Intervals.of("2023-01-01/2023-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );
    segmentSchemaTestUtils.insertUsedSegments(Collections.singleton(segment), Collections.emptyMap());

    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();
    RowSignature rowSignature = RowSignature.builder().add("cx", ColumnType.FLOAT).build();
    Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
    aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

    segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(new SchemaPayload(rowSignature, aggregatorFactoryMap), 20));
    segmentSchemaBackFillQueue.add(segment.getId(), rowSignature, aggregatorFactoryMap, 20);
    segmentSchemaBackFillQueue.onLeaderStart();
    latch.await();
    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }

  private CentralizedDatasourceSchemaConfig getEnabledConfig()
  {
    CentralizedDatasourceSchemaConfig config = new CentralizedDatasourceSchemaConfig();
    config.setEnabled(true);
    return config;
  }
}
