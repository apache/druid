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

package org.apache.druid.server.coordinator.duty;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SegmentsMetadataManager;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.metadata.FingerprintGenerator;
import org.apache.druid.segment.metadata.SegmentSchemaManager;
import org.apache.druid.segment.metadata.SegmentSchemaTestUtils;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.TestDruidCoordinatorConfig;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.skife.jdbi.v2.Update;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class KillUnreferencedSegmentSchemaDutyTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.create(true));

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  private TestDerbyConnector derbyConnector;
  private MetadataStorageTablesConfig tablesConfig;
  private SegmentSchemaManager segmentSchemaManager;
  private FingerprintGenerator fingerprintGenerator;
  private SegmentSchemaTestUtils segmentSchemaTestUtils;
  @Mock
  private DruidCoordinatorRuntimeParams mockDruidCoordinatorRuntimeParams;

  @Before
  public void setUp()
  {
    derbyConnector = derbyConnectorRule.getConnector();
    tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    derbyConnector.createSegmentSchemaTable();
    derbyConnector.createSegmentTable();

    fingerprintGenerator = new FingerprintGenerator(mapper);
    segmentSchemaManager = new SegmentSchemaManager(derbyConnectorRule.metadataTablesConfigSupplier().get(), mapper, derbyConnector);
    segmentSchemaTestUtils = new SegmentSchemaTestUtils(derbyConnectorRule, derbyConnector, mapper);
    CoordinatorRunStats runStats = new CoordinatorRunStats();
    Mockito.when(mockDruidCoordinatorRuntimeParams.getCoordinatorStats()).thenReturn(runStats);
  }

  @Test
  public void testKillUnreferencedSchema()
  {
    List<DateTime> dateTimes = new ArrayList<>();

    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));
    dateTimes.add(now.plusMinutes(6 * 60 + 1));

    SegmentsMetadataManager segmentsMetadataManager = Mockito.mock(SegmentsMetadataManager.class);
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillDurationToRetain(Period.parse("PT6H").toStandardDuration())
        .build();

    KillUnreferencedSegmentSchemaDuty duty =
        new TestKillUnreferencedSegmentSchemasDuty(druidCoordinatorConfig, segmentSchemaManager, segmentsMetadataManager, dateTimes);

    Set<DataSegment> segments = new HashSet<>();
    List<SegmentSchemaManager.SegmentSchemaMetadataPlus> schemaMetadataPluses = new ArrayList<>();

    RowSignature rowSignature = RowSignature.builder().add("c1", ColumnType.FLOAT).build();

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
    SchemaPayloadPlus schemaMetadata = new SchemaPayloadPlus(schemaPayload, (long) 1);

    DataSegment segment1 = new DataSegment(
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

    DataSegment segment2 = new DataSegment(
        "foo",
        Intervals.of("2023-01-02/2023-01-03"),
        "2023-01-02",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    segments.add(segment1);
    segments.add(segment2);

    SegmentSchemaManager.SegmentSchemaMetadataPlus plus1 =
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment1.getId(),
            fingerprintGenerator.generateFingerprint(schemaPayload),
            schemaMetadata
        );
    schemaMetadataPluses.add(plus1);

    SegmentSchemaManager.SegmentSchemaMetadataPlus plus2 =
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment2.getId(),
            fingerprintGenerator.generateFingerprint(schemaPayload),
            schemaMetadata
        );
    schemaMetadataPluses.add(plus2);

    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());
    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable("foo", schemaMetadataPluses, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    List<Long> schemaIds = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(StringUtils.format(
            "SELECT schema_id from %s where id = '%s'",
            tablesConfig.getSegmentsTable(), schemaMetadataPluses.get(0).getSegmentId().toString()
        )).mapTo(Long.class).list());

    long schemaIdToDelete = schemaIds.get(0);

    // delete segment1
    derbyConnector.retryWithHandle(handle -> {
      Update deleteStatement = handle.createStatement(
          StringUtils.format(
              "DELETE FROM %s WHERE id = '%s'",
              tablesConfig.getSegmentsTable(),
              schemaMetadataPluses.get(0).getSegmentId().toString()
          )
      );
      deleteStatement.execute();
      return null;
    });

    // this call should do nothing
    duty.run(mockDruidCoordinatorRuntimeParams);

    List<Boolean> usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(StringUtils.format(
            "SELECT used from %s where id = %s",
            tablesConfig.getSegmentSchemasTable(), schemaIdToDelete
        )).mapTo(Boolean.class).list()
    );

    Assert.assertTrue(usedStatus.get(0));

    // delete segment2
    derbyConnector.retryWithHandle(handle -> {
      Update deleteStatement = handle.createStatement(
          StringUtils.format(
              "DELETE FROM %s WHERE id = '%s'",
              tablesConfig.getSegmentsTable(),
              schemaMetadataPluses.get(1).getSegmentId().toString()
          )
      );
      deleteStatement.execute();
      return null;
    });

    // this call should mark the schema unused
    duty.run(mockDruidCoordinatorRuntimeParams);

    usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(StringUtils.format(
            "SELECT used from %s where id = %s",
            tablesConfig.getSegmentSchemasTable(), schemaIdToDelete
        )).mapTo(Boolean.class).list()
    );

    Assert.assertFalse(usedStatus.get(0));

    // this call should delete the schema
    duty.run(mockDruidCoordinatorRuntimeParams);

    usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(StringUtils.format(
            "SELECT used from %s where id = %s",
            tablesConfig.getSegmentSchemasTable(), schemaIdToDelete
        )).mapTo(Boolean.class).list()
    );

    Assert.assertTrue(usedStatus.isEmpty());
  }

  @Test
  public void testKillUnreferencedSchema_repair()
  {
    List<DateTime> dateTimes = new ArrayList<>();

    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));

    SegmentsMetadataManager segmentsMetadataManager = Mockito.mock(SegmentsMetadataManager.class);
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillDurationToRetain(Period.parse("PT6H").toStandardDuration())
        .build();

    KillUnreferencedSegmentSchemaDuty duty =
        new TestKillUnreferencedSegmentSchemasDuty(druidCoordinatorConfig, segmentSchemaManager, segmentsMetadataManager, dateTimes);

    RowSignature rowSignature = RowSignature.builder().add("c1", ColumnType.FLOAT).build();

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
    String fingerprint = fingerprintGenerator.generateFingerprint(schemaPayload);

    derbyConnector.retryWithHandle(
        handle -> {
          segmentSchemaManager.persistSegmentSchema(
              handle,
              "foo",
              Collections.singletonMap(fingerprint, schemaPayload),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          );
          return null;
        }
    );

    List<Long> schemaIds = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT id from %s where fingerprint = '%s' and datasource = 'foo'",
                tablesConfig.getSegmentSchemasTable(), fingerprint
            )).mapTo(Long.class).list()
    );

    long schemaId = schemaIds.get(0);

    List<Boolean> usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT used from %s where id = %s",
                tablesConfig.getSegmentSchemasTable(), schemaId
            )
        ).mapTo(Boolean.class).list()
    );

    Assert.assertTrue(usedStatus.get(0));

    // this call should mark the schema as unused
    duty.run(mockDruidCoordinatorRuntimeParams);

    usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT used from %s where id = %s",
                tablesConfig.getSegmentSchemasTable(), schemaId
            )
        ).mapTo(Boolean.class).list()
    );

    Assert.assertFalse(usedStatus.get(0));

    // associate a segment to the schema
    DataSegment segment1 = new DataSegment(
        "foo",
        Intervals.of("2023-01-02/2023-01-03"),
        "2023-01-02",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    segmentSchemaTestUtils.insertUsedSegments(Collections.singleton(segment1), Collections.emptyMap());

    derbyConnector.retryWithHandle(
        handle -> handle.createStatement(
            StringUtils.format(
                "UPDATE %s SET schema_id = %s, num_rows = 100 WHERE id = '%s'",
                tablesConfig.getSegmentsTable(), schemaId, segment1.getId().toString()
            )).execute()
    );

    // this call should make the schema used
    duty.run(mockDruidCoordinatorRuntimeParams);

    usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT used from %s where id = %s",
                tablesConfig.getSegmentSchemasTable(), schemaId
            )).mapTo(Boolean.class).list()
    );

    Assert.assertTrue(usedStatus.get(0));
  }

  @Test
  public void testKillOlderVersionSchema()
  {
    List<DateTime> dateTimes = new ArrayList<>();

    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));
    dateTimes.add(now.plusMinutes(6 * 60 + 1));

    SegmentsMetadataManager segmentsMetadataManager = Mockito.mock(SegmentsMetadataManager.class);
    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillDurationToRetain(Period.parse("PT6H").toStandardDuration())
        .build();

    KillUnreferencedSegmentSchemaDuty duty =
        new TestKillUnreferencedSegmentSchemasDuty(druidCoordinatorConfig, segmentSchemaManager, segmentsMetadataManager, dateTimes);

    // create 2 versions of same schema
    // unreferenced one should get deleted
    RowSignature rowSignature = RowSignature.builder().add("c1", ColumnType.FLOAT).build();

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
    String fingerprint = fingerprintGenerator.generateFingerprint(schemaPayload);

    derbyConnector.retryWithHandle(
        handle -> {
          segmentSchemaManager.persistSegmentSchema(
              handle,
              "foo",
              Collections.singletonMap(fingerprint, schemaPayload),
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
          );
          return null;
        }
    );

    derbyConnector.retryWithHandle(
        handle -> {
          segmentSchemaManager.persistSegmentSchema(
              handle,
              "foo",
              Collections.singletonMap(fingerprint, schemaPayload),
              0
          );
          return null;
        }
    );

    List<Long> schemaIds = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT id from %s where fingerprint = '%s' and datasource = 'foo' and version = %s",
                tablesConfig.getSegmentSchemasTable(), fingerprint, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            )).mapTo(Long.class).list()
    );

    long schemaIdNewVersion = schemaIds.get(0);

    schemaIds = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT id from %s where fingerprint = '%s' and datasource = 'foo' and version = %s",
                tablesConfig.getSegmentSchemasTable(), fingerprint, 0
            )).mapTo(Long.class).list()
    );

    long schemaIdOldVersion = schemaIds.get(0);

    // this call should mark both the schema as unused
    duty.run(mockDruidCoordinatorRuntimeParams);

    List<Boolean> usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT used from %s where id = %s",
                tablesConfig.getSegmentSchemasTable(), schemaIdNewVersion
            )
        ).mapTo(Boolean.class).list()
    );

    Assert.assertFalse(usedStatus.get(0));

    // associate a segment to the schema
    DataSegment segment1 = new DataSegment(
        "foo",
        Intervals.of("2023-01-02/2023-01-03"),
        "2023-01-02",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    segmentSchemaTestUtils.insertUsedSegments(Collections.singleton(segment1), Collections.emptyMap());

    derbyConnector.retryWithHandle(
        handle -> handle.createStatement(
            StringUtils.format(
                "UPDATE %s SET schema_id = %s, num_rows = 100 WHERE id = '%s'",
                tablesConfig.getSegmentsTable(), schemaIdNewVersion, segment1.getId().toString()
            )).execute()
    );

    // this call should make the referenced schema used
    duty.run(mockDruidCoordinatorRuntimeParams);

    usedStatus = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT used from %s where id = %s",
                tablesConfig.getSegmentSchemasTable(), schemaIdNewVersion
            )).mapTo(Boolean.class).list()
    );

    Assert.assertTrue(usedStatus.get(0));

    // this call should kill the schema
    duty.run(mockDruidCoordinatorRuntimeParams);

    List<Integer> counts = derbyConnector.retryWithHandle(
        handle -> handle.createQuery(
            StringUtils.format(
                "SELECT count(*) from %s where id = %s",
                tablesConfig.getSegmentSchemasTable(), schemaIdOldVersion
            )).mapTo(Integer.class).list()
    );

    Assert.assertEquals(0, counts.get(0).intValue());
  }

  private static class TestKillUnreferencedSegmentSchemasDuty extends KillUnreferencedSegmentSchemaDuty
  {
    private final List<DateTime> dateTimes;
    private int index = -1;

    public TestKillUnreferencedSegmentSchemasDuty(
        DruidCoordinatorConfig config,
        SegmentSchemaManager segmentSchemaManager,
        SegmentsMetadataManager segmentsMetadataManager,
        List<DateTime> dateTimes
    )
    {
      super(config, segmentSchemaManager, segmentsMetadataManager);
      this.dateTimes = dateTimes;
    }

    @Override
    protected DateTime getCurrentTime()
    {
      index++;
      return dateTimes.get(index);
    }
  }
}
