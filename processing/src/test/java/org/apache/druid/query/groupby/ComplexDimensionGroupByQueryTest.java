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

package org.apache.druid.query.groupby;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.SerializablePairLongStringComplexMetricSerde;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class ComplexDimensionGroupByQueryTest
{
  private final QueryContexts.Vectorize vectorize;
  private final AggregationTestHelper helper;
  private final List<Segment> segments;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public ComplexDimensionGroupByQueryTest(GroupByQueryConfig config, String vectorize)
  {
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Collections.emptyList(),
        config,
        tempFolder
    );
    Sequence<Object[]> rows = Sequences.simple(
        ImmutableList.of(
            new Object[]{new SerializablePairLongString(1L, "abc")},
            new Object[]{new SerializablePairLongString(1L, "abc")},
            new Object[]{new SerializablePairLongString(1L, "def")},
            new Object[]{new SerializablePairLongString(1L, "abc")},
            new Object[]{new SerializablePairLongString(1L, "ghi")},
            new Object[]{new SerializablePairLongString(1L, "def")},
            new Object[]{new SerializablePairLongString(1L, "abc")},
            new Object[]{new SerializablePairLongString(1L, "pqr")},
            new Object[]{new SerializablePairLongString(1L, "xyz")},
            new Object[]{new SerializablePairLongString(1L, "foo")},
            new Object[]{new SerializablePairLongString(1L, "bar")}
        )
    );
    RowSignature rowSignature = RowSignature.builder()
                                            .add(
                                                "pair",
                                                ColumnType.ofComplex(SerializablePairLongStringComplexMetricSerde.TYPE_NAME)
                                            )
                                            .build();

    this.segments = Collections.singletonList(
        new RowBasedSegment<>(
            SegmentId.dummy("dummy"),
            rows,
            columnName -> {
              final int columnNumber = rowSignature.indexOf(columnName);
              return row -> columnNumber >= 0 ? row[columnNumber] : null;
            },
            rowSignature
        )
    );
  }

  @Parameterized.Parameters(name = "config = {0}, vectorize = {1}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "force"}) {
        constructors.add(new Object[]{config, vectorize});
      }
    }
    return constructors;
  }

  public Map<String, Object> getContext()
  {
    return ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize.toString(),
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, "true"
    );
  }

  @Test
  public void testGroupByOnPairClass()
  {
    GroupByQuery groupQuery = GroupByQuery.builder()
                                          .setDataSource("test_datasource")
                                          .setGranularity(Granularities.ALL)
                                          .setInterval(Intervals.ETERNITY)
                                          .setDimensions(new DefaultDimensionSpec(
                                              "pair",
                                              "pair",
                                              ColumnType.ofComplex(SerializablePairLongStringComplexMetricSerde.TYPE_NAME)
                                          ))
                                          .setAggregatorSpecs(new CountAggregatorFactory("count"))
                                          .setContext(getContext())
                                          .build();

    if (vectorize == QueryContexts.Vectorize.FORCE) {
      // Cannot vectorize group by on complex dimension
      Assert.assertThrows(
          RuntimeException.class,
          () -> helper.runQueryOnSegmentsObjs(segments, groupQuery).toList()
      );
    } else {
      List<ResultRow> resultRows = helper.runQueryOnSegmentsObjs(segments, groupQuery).toList();

      Assert.assertArrayEquals(
          new ResultRow[]{
              ResultRow.of(new SerializablePairLongString(1L, "abc"), 4L),
              ResultRow.of(new SerializablePairLongString(1L, "bar"), 1L),
              ResultRow.of(new SerializablePairLongString(1L, "def"), 2L),
              ResultRow.of(new SerializablePairLongString(1L, "foo"), 1L),
              ResultRow.of(new SerializablePairLongString(1L, "ghi"), 1L),
              ResultRow.of(new SerializablePairLongString(1L, "pqr"), 1L),
              ResultRow.of(new SerializablePairLongString(1L, "xyz"), 1L)
          },
          resultRows.toArray()
      );
    }
  }
}
