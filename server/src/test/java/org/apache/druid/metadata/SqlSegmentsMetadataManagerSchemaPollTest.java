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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.SchemaPayload;
import org.apache.druid.segment.column.SegmentSchemaMetadata;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.SchemaFingerprintGenerator;
import org.apache.druid.segment.metadata.SchemaManager;
import org.apache.druid.segment.metadata.SegmentSchemaCache;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SqlSegmentsMetadataManagerSchemaPollTest extends SqlSegmentsMetadataManagerCommon
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule(getEnabledConfig());

  @Before
  public void setUp() throws Exception
  {
    connector = derbyConnectorRule.getConnector();
    SegmentsMetadataManagerConfig config = new SegmentsMetadataManagerConfig();
    config.setPollDuration(Period.seconds(3));

    segmentSchemaCache = new SegmentSchemaCache();
    schemaManager = new SchemaManager(
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        jsonMapper,
        connector
    );

    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector,
        segmentSchemaCache,
        CentralizedDatasourceSchemaConfig.create()
    );
    sqlSegmentsMetadataManager.start();

    publisher = new SQLMetadataSegmentPublisher(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        connector
    );

    connector.createSegmentSchemaTable();
    connector.createSegmentTable();

    publisher.publishSegment(segment1);
    publisher.publishSegment(segment2);
  }

  @After
  public void teardown()
  {
    if (sqlSegmentsMetadataManager.isPollingDatabasePeriodically()) {
      sqlSegmentsMetadataManager.stopPollingDatabasePeriodically();
    }
    sqlSegmentsMetadataManager.stop();
  }

  @Test(timeout = 60_000)
  public void testPollSegmentAndSchema()
  {
    List<SchemaManager.SegmentSchemaMetadataPlus> list = new ArrayList<>();
    SchemaFingerprintGenerator fingerprintGenerator = new SchemaFingerprintGenerator(jsonMapper);
    SchemaPayload payload1 = new SchemaPayload(
        RowSignature.builder().add("c1", ColumnType.FLOAT).build());
    SegmentSchemaMetadata schemaMetadata1 = new SegmentSchemaMetadata(payload1, 20L);
    list.add(new SchemaManager.SegmentSchemaMetadataPlus(segment1.getId(), schemaMetadata1, fingerprintGenerator.generateId(payload1)));
    SchemaPayload payload2 = new SchemaPayload(
        RowSignature.builder().add("c2", ColumnType.FLOAT).build());
    SegmentSchemaMetadata schemaMetadata2 = new SegmentSchemaMetadata(payload2, 40L);
    list.add(new SchemaManager.SegmentSchemaMetadataPlus(segment2.getId(), schemaMetadata2, fingerprintGenerator.generateId(payload2)));

    schemaManager.persistSchemaAndUpdateSegmentsTable(list);

    CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig = new CentralizedDatasourceSchemaConfig();
    centralizedDatasourceSchemaConfig.setEnabled(true);
    config = new SegmentsMetadataManagerConfig();
    config.setPollDuration(Period.seconds(3));
    sqlSegmentsMetadataManager = new SqlSegmentsMetadataManager(
        jsonMapper,
        Suppliers.ofInstance(config),
        derbyConnectorRule.metadataTablesConfigSupplier(),
        connector,
        segmentSchemaCache,
        centralizedDatasourceSchemaConfig
    );

    sqlSegmentsMetadataManager.start();
    DataSourcesSnapshot dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertNull(dataSourcesSnapshot);
    Assert.assertFalse(segmentSchemaCache.getSchemaForSegment(segment1.getId()).isPresent());
    Assert.assertFalse(segmentSchemaCache.getSchemaForSegment(segment2.getId()).isPresent());

    sqlSegmentsMetadataManager.startPollingDatabasePeriodically();
    Assert.assertTrue(sqlSegmentsMetadataManager.isPollingDatabasePeriodically());
    // This call make sure that the first poll is completed
    sqlSegmentsMetadataManager.useLatestSnapshotIfWithinDelay();
    Assert.assertTrue(sqlSegmentsMetadataManager.getLatestDatabasePoll() instanceof SqlSegmentsMetadataManager.PeriodicDatabasePoll);
    Assert.assertTrue(segmentSchemaCache.getSchemaForSegment(segment1.getId()).isPresent());
    Assert.assertTrue(segmentSchemaCache.getSchemaForSegment(segment2.getId()).isPresent());

    Assert.assertEquals(schemaMetadata1, segmentSchemaCache.getSchemaForSegment(segment1.getId()).get());
    Assert.assertEquals(schemaMetadata2, segmentSchemaCache.getSchemaForSegment(segment2.getId()).get());

    dataSourcesSnapshot = sqlSegmentsMetadataManager.getDataSourcesSnapshot();
    Assert.assertEquals(
        ImmutableSet.of("wikipedia"),
        sqlSegmentsMetadataManager.retrieveAllDataSourceNames()
    );
    Assert.assertEquals(
        ImmutableList.of("wikipedia"),
        dataSourcesSnapshot.getDataSourcesWithAllUsedSegments()
                           .stream()
                           .map(ImmutableDruidDataSource::getName)
                           .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.getDataSource("wikipedia").getSegments())
    );
    Assert.assertEquals(
        ImmutableSet.of(segment1, segment2),
        ImmutableSet.copyOf(dataSourcesSnapshot.iterateAllUsedSegmentsInSnapshot())
    );
  }

  private CentralizedDatasourceSchemaConfig getEnabledConfig()
  {
    CentralizedDatasourceSchemaConfig config = CentralizedDatasourceSchemaConfig.create();
    config.setEnabled(true);
    return config;
  }
}
