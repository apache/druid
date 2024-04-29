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
import org.apache.druid.timeline.SegmentId;
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
import org.skife.jdbi.v2.tweak.HandleCallback;

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

    derbyConnector.createSegmentSchemasTable();
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

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillDurationToRetain(Period.parse("PT6H").toStandardDuration())
        .build();

    KillUnreferencedSegmentSchemaDuty duty =
        new TestKillUnreferencedSegmentSchemasDuty(druidCoordinatorConfig, segmentSchemaManager, dateTimes);

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

    String fingerprint =
        fingerprintGenerator.generateFingerprint(
            schemaPayload,
            segment1.getDataSource(),
            CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
        );

    SegmentSchemaManager.SegmentSchemaMetadataPlus plus1 =
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment1.getId(),
            fingerprint,
            schemaMetadata
        );
    schemaMetadataPluses.add(plus1);

    SegmentSchemaManager.SegmentSchemaMetadataPlus plus2 =
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            segment2.getId(),
            fingerprint,
            schemaMetadata
        );
    schemaMetadataPluses.add(plus2);

    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());
    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable(
        "foo",
        schemaMetadataPluses,
        CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
    );

    // delete segment1
    deleteSegment(schemaMetadataPluses.get(0).getSegmentId());

    // this call should do nothing
    duty.run(mockDruidCoordinatorRuntimeParams);

    Assert.assertEquals(Boolean.TRUE, getSchemaUsedStatus(fingerprint));

    // delete segment2
    deleteSegment(schemaMetadataPluses.get(1).getSegmentId());

    // this call should mark the schema unused
    duty.run(mockDruidCoordinatorRuntimeParams);

    Assert.assertEquals(Boolean.FALSE, getSchemaUsedStatus(fingerprint));

    // this call should delete the schema
    duty.run(mockDruidCoordinatorRuntimeParams);


    Assert.assertNull(getSchemaUsedStatus(fingerprint));
  }

  @Test
  public void testKillUnreferencedSchema_repair()
  {
    List<DateTime> dateTimes = new ArrayList<>();

    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillDurationToRetain(Period.parse("PT6H").toStandardDuration())
        .build();

    KillUnreferencedSegmentSchemaDuty duty =
        new TestKillUnreferencedSegmentSchemasDuty(druidCoordinatorConfig, segmentSchemaManager, dateTimes);

    RowSignature rowSignature = RowSignature.builder().add("c1", ColumnType.FLOAT).build();

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
    String fingerprint = fingerprintGenerator.generateFingerprint(schemaPayload, "foo", CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    inHandle(
        handle -> {
          segmentSchemaManager.persistSegmentSchema(
              handle,
              "foo",
              CentralizedDatasourceSchemaConfig.SCHEMA_VERSION,
              Collections.singletonMap(fingerprint, schemaPayload)
          );
          return null;
        }
    );

    Assert.assertEquals(Boolean.TRUE, getSchemaUsedStatus(fingerprint));

    // this call should mark the schema as unused
    duty.run(mockDruidCoordinatorRuntimeParams);

    Assert.assertEquals(Boolean.FALSE, getSchemaUsedStatus(fingerprint));

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

    inHandle(
        handle -> handle.createStatement(
            StringUtils.format(
                "UPDATE %s SET schema_fingerprint = '%s', num_rows = 100 WHERE id = '%s'",
                tablesConfig.getSegmentsTable(), fingerprint, segment1.getId().toString()
            )).execute()
    );

    // this call should make the schema used
    duty.run(mockDruidCoordinatorRuntimeParams);

    Assert.assertEquals(Boolean.TRUE, getSchemaUsedStatus(fingerprint));
  }

  @Test
  public void testKillOlderVersionSchema()
  {
    List<DateTime> dateTimes = new ArrayList<>();

    DateTime now = DateTimes.nowUtc();
    dateTimes.add(now);
    dateTimes.add(now.plusMinutes(61));
    dateTimes.add(now.plusMinutes(6 * 60 + 1));

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig.Builder()
        .withMetadataStoreManagementPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillPeriod(Period.parse("PT1H").toStandardDuration())
        .withSegmentSchemaKillDurationToRetain(Period.parse("PT6H").toStandardDuration())
        .build();

    KillUnreferencedSegmentSchemaDuty duty =
        new TestKillUnreferencedSegmentSchemasDuty(druidCoordinatorConfig, segmentSchemaManager, dateTimes);

    // create 2 versions of same schema
    // unreferenced one should get deleted
    RowSignature rowSignature = RowSignature.builder().add("c1", ColumnType.FLOAT).build();

    SchemaPayload schemaPayload = new SchemaPayload(rowSignature);
    String fingerprintOldVersion = fingerprintGenerator.generateFingerprint(schemaPayload, "foo", 0);
    String fingerprintNewVersion = fingerprintGenerator.generateFingerprint(schemaPayload, "foo", 1);

    inHandle(
        handle -> {
          segmentSchemaManager.persistSegmentSchema(
              handle,
              "foo",
              0,
              Collections.singletonMap(fingerprintOldVersion, schemaPayload)
          );
          return null;
        }
    );

    inHandle(
        handle -> {
          segmentSchemaManager.persistSegmentSchema(
              handle,
              "foo",
              1,
              Collections.singletonMap(fingerprintNewVersion, schemaPayload)
          );
          return null;
        }
    );

    // this call should mark both the schema as unused
    duty.run(mockDruidCoordinatorRuntimeParams);

    Assert.assertEquals(Boolean.FALSE, getSchemaUsedStatus(fingerprintOldVersion));
    Assert.assertEquals(Boolean.FALSE, getSchemaUsedStatus(fingerprintNewVersion));

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

    inHandle(
        handle -> handle.createStatement(
            StringUtils.format(
                "UPDATE %s SET schema_fingerprint = '%s', num_rows = 100 WHERE id = '%s'",
                tablesConfig.getSegmentsTable(), fingerprintNewVersion, segment1.getId().toString()
            )).execute()
    );

    // this call should make the referenced schema used
    duty.run(mockDruidCoordinatorRuntimeParams);

    Assert.assertEquals(Boolean.TRUE, getSchemaUsedStatus(fingerprintNewVersion));

    // this call should kill the schema
    duty.run(mockDruidCoordinatorRuntimeParams);
    Assert.assertNull(getSchemaUsedStatus(fingerprintOldVersion));
  }

  private static class TestKillUnreferencedSegmentSchemasDuty extends KillUnreferencedSegmentSchemaDuty
  {
    private final List<DateTime> dateTimes;
    private int index = -1;

    public TestKillUnreferencedSegmentSchemasDuty(
        DruidCoordinatorConfig config,
        SegmentSchemaManager segmentSchemaManager,
        List<DateTime> dateTimes
    )
    {
      super(config, segmentSchemaManager);
      this.dateTimes = dateTimes;
    }

    @Override
    protected DateTime getCurrentTime()
    {
      index++;
      return dateTimes.get(index);
    }
  }

  private <T> T inHandle(HandleCallback<T> callback)
  {
    return derbyConnector.retryWithHandle(callback);
  }

  private void deleteSegment(SegmentId id)
  {
    inHandle(handle -> {
      Update deleteStatement = handle.createStatement(
          StringUtils.format(
              "DELETE FROM %s WHERE id = '%s'",
              tablesConfig.getSegmentsTable(),
              id.toString()
          )
      );
      deleteStatement.execute();
      return null;
    });
  }

  private Boolean getSchemaUsedStatus(String fingerprint)
  {
    List<Boolean> usedStatus = inHandle(
        handle -> handle.createQuery(StringUtils.format(
            "SELECT used from %s where fingerprint = '%s'",
            tablesConfig.getSegmentSchemasTable(), fingerprint
        )).mapTo(Boolean.class).list()
    );

    return usedStatus.isEmpty() ? null : usedStatus.get(0);
  }
}
