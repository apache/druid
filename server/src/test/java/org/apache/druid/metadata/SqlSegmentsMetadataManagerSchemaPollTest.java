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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataManagerSchemaPollTest extends SqlSegmentsMetadataManagerTestBase
{
  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.enabled(true));

  @BeforeEach
  public void setUp() throws Exception
  {
    setUp(derbyConnectorRule);
    segmentSchemaCache = new SegmentSchemaCache();
    segmentSchemaManager = new SegmentSchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        jsonMapper,
        connector
    );

    publishSegment(segment1);
    publishSegment(segment2);
  }

  @AfterEach
  public void teardown()
  {
    teardownManager();
  }

  @Test
  @Timeout(value = 60_000, unit = TimeUnit.MILLISECONDS)
  public void testPollSegmentAndSchema()
  {
    List<SegmentSchemaManager.SegmentSchemaMetadataPlus> list = new ArrayList<>();
    FingerprintGenerator fingerprintGenerator = new FingerprintGenerator(jsonMapper);
    SchemaPayload payload1 = new SchemaPayload(
        RowSignature.builder().add("c1", ColumnType.FLOAT).build());
    SchemaPayloadPlus schemaMetadata1 = new SchemaPayloadPlus(payload1, 20L);
    list.add(
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment1.getId(),
            fingerprintGenerator.generateFingerprint(
                payload1,
                "wikipedia",
                CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            ),
            schemaMetadata1
        )
    );
    SchemaPayload payload2 = new SchemaPayload(
        RowSignature.builder().add("c2", ColumnType.FLOAT).build());
    SchemaPayloadPlus schemaMetadata2 = new SchemaPayloadPlus(payload2, 40L);
    list.add(
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment2.getId(),
            fingerprintGenerator.generateFingerprint(
                payload2,
                "wikipedia",
                CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            ),
            schemaMetadata2
        )
    );

    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable("wikipedia", list, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
        = CentralizedDatasourceSchemaConfig.enabled(true);
    config = new SegmentsMetadataManagerConfig(Period.seconds(3), null, null);
    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector,
        segmentSchemaCache,
        centralizedDatasourceSchemaConfig,
        NoopServiceEmitter.instance()
    );

    sqlSegmentsMetadataManager.start();
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assertions.assertNull(dataSourcesSnapshot);
    Assertions.assertFalse(segmentSchemaCache.getSchemaForSegment(segment1.getId()).isPresent());
    Assertions.assertFalse(segmentSchemaCache.getSchemaForSegment(segment2.getId()).isPresent());
    Assertions.assertFalse(segmentSchemaCache.isInitialized());

    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assertions.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    // This call make sure that the first poll is completed
    sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay();
    Assertions.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    Assertions.assertTrue(segmentSchemaCache.isInitialized());
    Assertions.assertTrue(segmentSchemaCache.getSchemaForSegment(segment1.getId()).isPresent());
    Assertions.assertTrue(segmentSchemaCache.getSchemaForSegment(segment2.getId()).isPresent());

    Assertions.assertEquals(schemaMetadata1, segmentSchemaCache.getSchemaForSegment(segment1.getId()).get());
    Assertions.assertEquals(schemaMetadata2, segmentSchemaCache.getSchemaForSegment(segment2.getId()).get());

    dataSourcesSnapshot = sqlSegmentsMetadataManager.getLatestDataSourcesSnapshot();
    Assertions.assertEquals(
        ImmutableList.of("wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    Assertions.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.getDataSource("wikipedia").getSegments())
    );
    Assertions.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot())
    );
  }

  @Test
  public void testPollOnlyNewSchemaVersion()
  {
    List<SegmentSchemaManager.SegmentSchemaMetadataPlus> list = new ArrayList<>();
    FingerprintGenerator fingerprintGenerator = new FingerprintGenerator(jsonMapper);
    SchemaPayload payload1 = new SchemaPayload(
        RowSignature.builder().add("c1", ColumnType.FLOAT).build());
    SchemaPayloadPlus schemaMetadata1 = new SchemaPayloadPlus(payload1, 20L);
    list.add(
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment1.getId(),
            fingerprintGenerator.generateFingerprint(
                payload1,
                segment1.getDataSource(),
                0
            ),
            schemaMetadata1)
    );
    SchemaPayload payload2 = new SchemaPayload(
        RowSignature.builder().add("c2", ColumnType.FLOAT).build());
    SchemaPayloadPlus schemaMetadata2 = new SchemaPayloadPlus(payload2, 40L);
    list.add(
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment2.getId(),
            fingerprintGenerator.generateFingerprint(
                payload2,
                segment2.getDataSource(),
                0
            ),
            schemaMetadata2)
    );

    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable("wikipedia", list, 0);

    CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
        = CentralizedDatasourceSchemaConfig.enabled(true);
    config = new SegmentsMetadataManagerConfig(Period.seconds(3), null, null);
    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector,
        segmentSchemaCache,
        centralizedDatasourceSchemaConfig,
        NoopServiceEmitter.instance()
    );

    sqlSegmentsMetadataManager.start();
    sqlSegmentsMetadataManager.poll();
    Assertions.assertTrue(segmentSchemaCache.isInitialized());
    Assertions.assertFalse(segmentSchemaCache.getSchemaForSegment(segment1.getId()).isPresent());
    Assertions.assertFalse(segmentSchemaCache.getSchemaForSegment(segment2.getId()).isPresent());

    list.clear();
    list.add(
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment1.getId(),
            fingerprintGenerator.generateFingerprint(
                payload1,
                segment1.getDataSource(),
                CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            ),
            schemaMetadata1)
    );
    list.add(
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment2.getId(),
            fingerprintGenerator.generateFingerprint(
                payload2,
                segment2.getDataSource(),
                CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            ),
            schemaMetadata2)
    );
    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable("wikipedia", list, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    sqlSegmentsMetadataManager.poll();
    Assertions.assertTrue(segmentSchemaCache.isInitialized());
    Assertions.assertTrue(segmentSchemaCache.getSchemaForSegment(segment1.getId()).isPresent());
    Assertions.assertTrue(segmentSchemaCache.getSchemaForSegment(segment2.getId()).isPresent());
  }
}
