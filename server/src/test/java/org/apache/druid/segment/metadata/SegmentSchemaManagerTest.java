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
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.segment.SchemaPayload;
import org.apache.druid.segment.SchemaPayloadPlus;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

public class SegmentSchemaManagerTest
{
  static {
    NullHandling.initializeForTests();
  }

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule(CentralizedDatasourceSchemaConfig.create(true));

  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  SegmentSchemaManager segmentSchemaManager;

  TestDerbyConnector derbyConnector;
  MetadataStorageTablesConfig tablesConfig;
  FingerprintGenerator fingerprintGenerator;
  SegmentSchemaTestUtils segmentSchemaTestUtils;

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
  }

  @Test
  public void testPersistSchemaAndUpdateSegmentsTable()
  {
    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();
    Random random = new Random(5);

    Set<DataSegment> segments = new HashSet<>();
    List<SegmentSchemaManager.SegmentSchemaMetadataPlus> schemaMetadataPluses = new ArrayList<>();

    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-0" + i + "/2023-01-0" + (i + 1)),
          "2023-01-0" + i,
          ImmutableMap.of("path", "a-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(0),
          9,
          100
      );

      segments.add(segment);

      int randomNum = random.nextInt();
      RowSignature rowSignature = RowSignature.builder().add("c" + randomNum, ColumnType.FLOAT).build();
      Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
      aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);
      SchemaPayloadPlus schemaMetadata = new SchemaPayloadPlus(schemaPayload, (long) randomNum);
      SegmentSchemaManager.SegmentSchemaMetadataPlus plus =
          new SegmentSchemaManager.SegmentSchemaMetadataPlus(
              segment.getId(),
              fingerprintGenerator.generateFingerprint(
                  schemaPayload,
                  segment.getDataSource(),
                  CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
              ),
              schemaMetadata
          );
      schemaMetadataPluses.add(plus);
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, randomNum));
    }

    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());
    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable("foo", schemaMetadataPluses, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);

    // associate a new segment with existing schema
    DataSegment segment = segments.stream().findAny().get();
    Pair<SchemaPayload, Integer> schemaPayloadIntegerPair = segmentIdSchemaMap.get(segment.getId().toString());

    final DataSegment newSegment = new DataSegment(
        "foo",
        Intervals.of("2024-01-01/2024-01-02"),
        "2023-01-01",
        ImmutableMap.of("path", "a-1"),
        ImmutableList.of("dim1"),
        ImmutableList.of("m1"),
        new LinearShardSpec(0),
        9,
        100
    );

    SchemaPayloadPlus schemaMetadata =
        new SchemaPayloadPlus(
            schemaPayloadIntegerPair.lhs,
            500L
        );
    SegmentSchemaManager.SegmentSchemaMetadataPlus plus =
        new SegmentSchemaManager.SegmentSchemaMetadataPlus(
            newSegment.getId(),
            fingerprintGenerator.generateFingerprint(
                schemaPayloadIntegerPair.lhs,
                newSegment.getDataSource(),
                CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
            ),
            schemaMetadata
        );

    segmentSchemaTestUtils.insertUsedSegments(Collections.singleton(newSegment), Collections.emptyMap());
    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable(
        "foo",
        Collections.singletonList(plus),
        CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
    );

    segmentIdSchemaMap.clear();
    segmentIdSchemaMap.put(newSegment.getId().toString(), Pair.of(schemaPayloadIntegerPair.lhs, 500));

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }

  @Test
  public void testPersistAndUpdateSegmentsTable_unusedExistingSegment()
  {
    final Map<String, Pair<SchemaPayload, Integer>> segmentIdSchemaMap = new HashMap<>();

    Set<DataSegment> segments = new HashSet<>();
    List<SegmentSchemaManager.SegmentSchemaMetadataPlus> schemaMetadataPluses = new ArrayList<>();

    for (int i = 1; i < 9; i++) {
      final DataSegment segment = new DataSegment(
          "foo",
          Intervals.of("2023-01-0" + i + "/2023-01-0" + (i + 1)),
          "2023-01-0" + i,
          ImmutableMap.of("path", "a-" + i),
          ImmutableList.of("dim1"),
          ImmutableList.of("m1"),
          new LinearShardSpec(0),
          9,
          100
      );

      segments.add(segment);

      RowSignature rowSignature = RowSignature.builder().add("c" + i, ColumnType.FLOAT).build();
      Map<String, AggregatorFactory> aggregatorFactoryMap = new HashMap<>();
      aggregatorFactoryMap.put("longFirst", new LongFirstAggregatorFactory("longFirst", "long-col", null));

      SchemaPayload schemaPayload = new SchemaPayload(rowSignature, aggregatorFactoryMap);
      SchemaPayloadPlus schemaMetadata = new SchemaPayloadPlus(schemaPayload, (long) i);
      SegmentSchemaManager.SegmentSchemaMetadataPlus plus =
          new SegmentSchemaManager.SegmentSchemaMetadataPlus(
              segment.getId(),
              fingerprintGenerator.generateFingerprint(
                  schemaPayload,
                  segment.getDataSource(),
                  CentralizedDatasourceSchemaConfig.SCHEMA_VERSION
              ),
              schemaMetadata
          );
      schemaMetadataPluses.add(plus);
      segmentIdSchemaMap.put(segment.getId().toString(), Pair.of(schemaPayload, i));
    }

    segmentSchemaTestUtils.insertUsedSegments(segments, Collections.emptyMap());

    final Set<String> unusedFingerprints = new HashSet<>();
    final Map<String, SchemaPayload> schemaPayloadMapToPersist = new HashMap<>();

    // persist the schema
    for (int i = 1; i <= 6; i++) {
      SegmentSchemaManager.SegmentSchemaMetadataPlus plus = schemaMetadataPluses.get(i);
      schemaPayloadMapToPersist.put(plus.getFingerprint(), plus.getSegmentSchemaMetadata().getSchemaPayload());

      if (i <= 3) {
        unusedFingerprints.add(plus.getFingerprint());
      }
    }

    segmentSchemaTestUtils.insertSegmentSchema("foo", schemaPayloadMapToPersist, unusedFingerprints);

    segmentSchemaManager.persistSchemaAndUpdateSegmentsTable("foo", schemaMetadataPluses, CentralizedDatasourceSchemaConfig.SCHEMA_VERSION);

    segmentSchemaTestUtils.verifySegmentSchema(segmentIdSchemaMap);
  }
}
