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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas.SegmentSchema;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class SinkSchemaUtilTest
{
  @Test
  public void testComputeAbsoluteSchemaEmpty()
  {
    Assert.assertEquals(Optional.empty(), SinkSchemaUtil.computeAbsoluteSchema(new HashMap<>()));
  }

  @Test
  public void testComputeAbsoluteSchema()
  {
    Map<SegmentId, Pair<RowSignature, Integer>> sinkSchemaMap = new HashMap<>();

    SegmentId segment1 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        0
    );
    Map<String, ColumnType> columnTypeMap1 = Maps.newLinkedHashMap();
    columnTypeMap1.put("dim1", ColumnType.FLOAT);
    columnTypeMap1.put("dim2", ColumnType.UNKNOWN_COMPLEX);
    columnTypeMap1.put("dim3", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema1 = Pair.of(toRowSignature(columnTypeMap1), 20);
    sinkSchemaMap.put(segment1, schema1);

    SegmentId segment2 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        1
    );

    Map<String, ColumnType> columnTypeMap2 = Maps.newLinkedHashMap();
    columnTypeMap2.put("dim1", ColumnType.FLOAT);
    columnTypeMap2.put("dim2", ColumnType.LONG);
    columnTypeMap2.put("dim3", ColumnType.STRING);
    columnTypeMap2.put("dim4", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema2 = Pair.of(toRowSignature(columnTypeMap2), 40);
    sinkSchemaMap.put(segment2, schema2);

    Optional<SegmentSchemas> segmentSchemas = SinkSchemaUtil.computeAbsoluteSchema(sinkSchemaMap);

    Assert.assertTrue(segmentSchemas.isPresent());
    Assert.assertEquals(2, segmentSchemas.get().getSegmentSchemaList().size());

    Map<String, SegmentSchema> segmentSchemaMap = segmentSchemas.get().getSegmentSchemaList().stream().collect(
        Collectors.toMap(SegmentSchema::getSegmentId, v -> v));

    SegmentSchema segmentSchema1 = segmentSchemaMap.get(segment1.toString());

    Assert.assertEquals(20, segmentSchema1.getNumRows().intValue());
    Assert.assertEquals(segment1.toString(), segmentSchema1.getSegmentId());
    Assert.assertEquals("foo", segmentSchema1.getDataSource());
    Assert.assertFalse(segmentSchema1.isDelta());
    Assert.assertEquals(ImmutableList.of("dim1", "dim2", "dim3"), segmentSchema1.getNewColumns());
    Assert.assertEquals(columnTypeMap1, segmentSchema1.getColumnTypeMap());
    Assert.assertEquals(Collections.emptyList(), segmentSchema1.getUpdatedColumns());

    SegmentSchema segmentSchema2 = segmentSchemaMap.get(segment2.toString());
    Assert.assertEquals(40, segmentSchema2.getNumRows().intValue());
    Assert.assertEquals(segment2.toString(), segmentSchema2.getSegmentId());
    Assert.assertEquals("foo", segmentSchema2.getDataSource());
    Assert.assertFalse(segmentSchema2.isDelta());
    Assert.assertEquals(ImmutableList.of("dim1", "dim2", "dim3", "dim4"), segmentSchema2.getNewColumns());
    Assert.assertEquals(columnTypeMap2, segmentSchema2.getColumnTypeMap());
    Assert.assertEquals(Collections.emptyList(), segmentSchema2.getUpdatedColumns());
  }

  @Test
  public void testComputeSchemaChangeNoChange()
  {
    Map<SegmentId, Pair<RowSignature, Integer>> previousSinkSchemaMap = new HashMap<>();

    SegmentId segment1 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        0
    );
    Map<String, ColumnType> columnTypeMap1 = Maps.newLinkedHashMap();
    columnTypeMap1.put("dim1", ColumnType.FLOAT);
    columnTypeMap1.put("dim2", ColumnType.UNKNOWN_COMPLEX);
    columnTypeMap1.put("dim3", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema1 = Pair.of(toRowSignature(columnTypeMap1), 20);
    previousSinkSchemaMap.put(segment1, schema1);

    SegmentId segment2 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        1
    );

    Map<String, ColumnType> columnTypeMap2 = Maps.newLinkedHashMap();
    columnTypeMap2.put("dim1", ColumnType.FLOAT);
    columnTypeMap2.put("dim2", ColumnType.LONG);
    columnTypeMap2.put("dim3", ColumnType.STRING);
    columnTypeMap2.put("dim4", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema2 = Pair.of(toRowSignature(columnTypeMap2), 40);
    previousSinkSchemaMap.put(segment2, schema2);

    SegmentId segment3 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        2
    );

    Map<String, ColumnType> columnTypeMap3 = Maps.newLinkedHashMap();
    columnTypeMap2.put("dim1", ColumnType.FLOAT);
    columnTypeMap2.put("dim2", ColumnType.LONG);
    columnTypeMap2.put("dim3", ColumnType.STRING);
    columnTypeMap2.put("dim5", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema3 = Pair.of(toRowSignature(columnTypeMap3), 80);
    previousSinkSchemaMap.put(segment3, schema3);

    Assert.assertFalse(SinkSchemaUtil.computeSchemaChange(previousSinkSchemaMap, previousSinkSchemaMap).isPresent());
  }

  @Test
  public void testComputeSchemaChange()
  {
    Map<SegmentId, Pair<RowSignature, Integer>> previousSinkSchemaMap = new HashMap<>();

    SegmentId segment1 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        0
    );
    Map<String, ColumnType> columnTypeMap1 = Maps.newLinkedHashMap();
    columnTypeMap1.put("dim1", ColumnType.FLOAT);
    columnTypeMap1.put("dim2", ColumnType.UNKNOWN_COMPLEX);
    columnTypeMap1.put("dim3", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema1 = Pair.of(toRowSignature(columnTypeMap1), 20);
    previousSinkSchemaMap.put(segment1, schema1);

    SegmentId segment2 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        1
    );

    Map<String, ColumnType> columnTypeMap2 = Maps.newLinkedHashMap();
    columnTypeMap2.put("dim1", ColumnType.FLOAT);
    columnTypeMap2.put("dim2", ColumnType.LONG);
    columnTypeMap2.put("dim3", ColumnType.STRING);
    columnTypeMap2.put("dim4", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema2 = Pair.of(toRowSignature(columnTypeMap2), 40);
    previousSinkSchemaMap.put(segment2, schema2);

    SegmentId segment3 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        2
    );

    Map<String, ColumnType> columnTypeMap3 = Maps.newLinkedHashMap();
    columnTypeMap2.put("dim1", ColumnType.FLOAT);
    columnTypeMap2.put("dim2", ColumnType.LONG);
    columnTypeMap2.put("dim3", ColumnType.STRING);
    columnTypeMap2.put("dim5", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema3 = Pair.of(toRowSignature(columnTypeMap3), 80);
    previousSinkSchemaMap.put(segment3, schema3);

    Map<SegmentId, Pair<RowSignature, Integer>> currentSinkSchemaMap = new HashMap<>();

    // new columns and numRows changed for segment1
    Map<String, ColumnType> currColumnTypeMap1 = Maps.newLinkedHashMap();
    currColumnTypeMap1.put("dim1", ColumnType.DOUBLE);
    currColumnTypeMap1.put("dim2", ColumnType.NESTED_DATA);
    currColumnTypeMap1.put("dim4", ColumnType.NESTED_DATA);
    currColumnTypeMap1.put("dim5", ColumnType.STRING);
    Pair<RowSignature, Integer> currSchema1 = Pair.of(toRowSignature(currColumnTypeMap1), 50);
    currentSinkSchemaMap.put(segment1, currSchema1);

    // no change for segment2
    currentSinkSchemaMap.put(segment2, schema2);

    // numRows changes for segment3
    Pair<RowSignature, Integer> currSchema3 = Pair.of(toRowSignature(columnTypeMap3), 100);
    currentSinkSchemaMap.put(segment3, currSchema3);

    SegmentId segment4 = SegmentId.of(
        "foo",
        Intervals.of("2000-01-01T01:00:00.000Z/2000-01-01T02:00:00.000Z"),
        "v1",
        5
    );

    Map<String, ColumnType> columnTypeMap4 = Maps.newLinkedHashMap();
    columnTypeMap4.put("dim1", ColumnType.FLOAT);
    columnTypeMap4.put("dim2", ColumnType.LONG);
    columnTypeMap4.put("dim3", ColumnType.STRING);
    columnTypeMap4.put("dim4", ColumnType.NESTED_DATA);
    Pair<RowSignature, Integer> schema4 = Pair.of(toRowSignature(columnTypeMap4), 40);
    currentSinkSchemaMap.put(segment4, schema4);

    Optional<SegmentSchemas> segmentSchemasChange = SinkSchemaUtil.computeSchemaChange(previousSinkSchemaMap, currentSinkSchemaMap);

    Assert.assertTrue(segmentSchemasChange.isPresent());
    Assert.assertEquals(3, segmentSchemasChange.get().getSegmentSchemaList().size());
    Map<String, SegmentSchema> segmentSchemaMap = segmentSchemasChange.get().getSegmentSchemaList().stream().collect(
        Collectors.toMap(SegmentSchema::getSegmentId, v -> v));

    SegmentSchema segmentSchema1 = segmentSchemaMap.get(segment1.toString());
    Assert.assertEquals(segment1.toString(), segmentSchema1.getSegmentId());
    Assert.assertEquals("foo", segmentSchema1.getDataSource());
    Assert.assertTrue(segmentSchema1.isDelta());
    Assert.assertEquals(50, segmentSchema1.getNumRows().intValue());
    Assert.assertEquals(ImmutableList.of("dim4", "dim5"), segmentSchema1.getNewColumns());
    Assert.assertEquals(ImmutableList.of("dim1", "dim2"), segmentSchema1.getUpdatedColumns());
    Assert.assertEquals(currColumnTypeMap1, segmentSchema1.getColumnTypeMap());

    SegmentSchema segmentSchema2 = segmentSchemaMap.get(segment3.toString());
    Assert.assertEquals(segment3.toString(), segmentSchema2.getSegmentId());
    Assert.assertEquals("foo", segmentSchema2.getDataSource());
    Assert.assertTrue(segmentSchema2.isDelta());
    Assert.assertEquals(100, segmentSchema2.getNumRows().intValue());
    Assert.assertEquals(Collections.emptyList(), segmentSchema2.getNewColumns());
    Assert.assertEquals(Collections.emptyList(), segmentSchema2.getUpdatedColumns());
    Assert.assertEquals(new HashMap<>(), segmentSchema2.getColumnTypeMap());

    SegmentSchema segmentSchema3 = segmentSchemaMap.get(segment4.toString());
    Assert.assertEquals(segment4.toString(), segmentSchema3.getSegmentId());
    Assert.assertEquals("foo", segmentSchema3.getDataSource());
    Assert.assertFalse(segmentSchema3.isDelta());
    Assert.assertEquals(40, segmentSchema3.getNumRows().intValue());
    Assert.assertEquals(ImmutableList.of("dim1", "dim2", "dim3", "dim4"), segmentSchema3.getNewColumns());
    Assert.assertEquals(Collections.emptyList(), segmentSchema3.getUpdatedColumns());
    Assert.assertEquals(columnTypeMap4, segmentSchema3.getColumnTypeMap());
  }

  private RowSignature toRowSignature(Map<String, ColumnType> columnTypeMap)
  {
    RowSignature.Builder builder = RowSignature.builder();

    for (Map.Entry<String, ColumnType> entry : columnTypeMap.entrySet()) {
      builder.add(entry.getKey(), entry.getValue());
    }

    return builder.build();
  }
}
