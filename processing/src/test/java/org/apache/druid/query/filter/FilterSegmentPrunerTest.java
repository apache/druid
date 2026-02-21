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

package org.apache.druid.query.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.StringTuple;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.DimensionRangeShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

class FilterSegmentPrunerTest
{
  private final ObjectMapper mapper = TestHelper.makeJsonMapper();

  @Test
  void testSerde() throws JsonProcessingException
  {
    SegmentPruner pruner = new FilterSegmentPruner(
        new EqualityFilter("dim", ColumnType.STRING, "val", null),
        Collections.singleton("dim")
    );
    Assertions.assertEquals(
        pruner,
        mapper.readValue(mapper.writeValueAsString(pruner), SegmentPruner.class)
    );
  }

  @Test
  void testSerdeNullFieldsSet() throws JsonProcessingException
  {
    SegmentPruner pruner = new FilterSegmentPruner(
        new EqualityFilter("dim", ColumnType.STRING, "val", null),
        null
    );
    Assertions.assertEquals(
        pruner,
        mapper.readValue(mapper.writeValueAsString(pruner), SegmentPruner.class)
    );
  }

  @Test
  void testPrune()
  {
    DimFilter range_a = new RangeFilter("dim1", ColumnType.STRING, null, "aaa", null, null, null);
    DimFilter expression_b = new ExpressionDimFilter("dim2 == 'c'", null, TestExprMacroTable.INSTANCE);

    String interval1 = "2026-02-18T00:00:00Z/2026-02-19T00:00:00Z";
    String interval2 = "2026-02-19T00:00:00Z/2026-02-20T00:00:00Z";

    DataSegment seg1 = makeDataSegment(interval1, makeRange("dim1", 0, null, "abc"));
    DataSegment seg2 = makeDataSegment(interval1, makeRange("dim1", 1, "abc", "lmn"));
    DataSegment seg3 = makeDataSegment(interval1, makeRange("dim1", 2, "lmn", null));
    DataSegment seg4 = makeDataSegment(interval2, makeRange("dim2", 0, null, "b"));
    DataSegment seg5 = makeDataSegment(interval2, makeRange("dim2", 1, "b", "j"));
    DataSegment seg6 = makeDataSegment(interval2, makeRange("dim2", 2, "j", "s"));
    DataSegment seg7 = makeDataSegment(interval2, makeRange("dim2", 3, "s", "t"));

    List<DataSegment> segs = List.of(seg1, seg2, seg3, seg4, seg5, seg6, seg7);

    FilterSegmentPruner prunerRange = new FilterSegmentPruner(range_a, null);
    FilterSegmentPruner prunerEmptyFields = new FilterSegmentPruner(range_a, Collections.emptySet());
    FilterSegmentPruner prunerExpression = new FilterSegmentPruner(expression_b, null);

    Assertions.assertEquals(Set.of(seg1, seg4, seg5, seg6, seg7), prunerRange.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerExpression.prune(segs, Function.identity()));
    Assertions.assertEquals(Set.copyOf(segs), prunerEmptyFields.prune(segs, Function.identity()));
  }

  @Test
  void testEqualsAndHashcode()
  {
    EqualsVerifier.forClass(FilterSegmentPruner.class).usingGetClass().withIgnoredFields("rangeCache").verify();
  }

  private ShardSpec makeRange(
      String column,
      int partitionNumber,
      @Nullable String start,
      @Nullable String end
  )
  {
    return makeRange(
        List.of(column),
        partitionNumber,
        start == null ? null : StringTuple.create(start),
        end == null ? null : StringTuple.create(end)
    );
  }

  private ShardSpec makeRange(
      List<String> columns,
      int partitionNumber,
      @Nullable StringTuple start,
      @Nullable StringTuple end
  )
  {
    return new DimensionRangeShardSpec(
        columns,
        start,
        end,
        partitionNumber,
        0
    );
  }

  private DataSegment makeDataSegment(String intervalString, ShardSpec shardSpec)
  {
    Interval interval = Intervals.of(intervalString);
    return DataSegment.builder(SegmentId.of("prune-test", interval, "0", shardSpec))
                      .shardSpec(shardSpec)
                      .build();
  }
}