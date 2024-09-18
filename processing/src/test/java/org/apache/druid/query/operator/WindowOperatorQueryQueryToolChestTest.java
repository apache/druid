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

package org.apache.druid.query.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.ResultSerializationMode;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.operator.window.WindowOperatorFactory;
import org.apache.druid.query.operator.window.ranking.WindowRowNumberProcessor;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.spec.LegacySegmentSpec;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class WindowOperatorQueryQueryToolChestTest extends InitializedNullHandlingTest
{

  private final WindowOperatorQueryQueryToolChest toolchest = new WindowOperatorQueryQueryToolChest();

  @Test
  public void mergeResultsWithRowResultSerializationMode()
  {
    RowSignature inputSignature = RowSignature.builder()
                                              .add("length", ColumnType.LONG)
                                              .build();
    RowSignature outputSignature = RowSignature.builder()
                                               .addAll(inputSignature)
                                               .add("w0", ColumnType.LONG)
                                               .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            Druids.newScanQueryBuilder()
                  .dataSource(new TableDataSource("test"))
                  .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                  .columns("length")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(new HashMap<>())
                  .build()
        ),
        new LegacySegmentSpec(Intervals.ETERNITY),
        new HashMap<>(),
        outputSignature,
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        ),
        ImmutableList.of()
    );
    List results = toolchest.mergeResults(
        (queryPlus, responseContext) -> Sequences.simple(
            Collections.singletonList(
                MapOfColumnsRowsAndColumns.fromMap(
                    ImmutableMap.of("length", new IntArrayColumn(new int[]{1, 5, 10}))
                )
            )
        )
    ).run(QueryPlus.wrap(query)).toList();

    Assert.assertTrue(results.get(0) instanceof Object[]);
    Assert.assertEquals(3, results.size());
    List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{1, 1},
        new Object[]{5, 2},
        new Object[]{10, 3}
    );

    for (int i = 0; i < 3; ++i) {
      Assert.assertArrayEquals(expectedResults.get(i), (Object[]) results.get(i));
    }
  }

  @Test
  public void mergeResultsWithFrameResultSerializationMode()
  {
    RowSignature inputSignature = RowSignature.builder()
                                              .add("length", ColumnType.LONG)
                                              .build();
    RowSignature outputSignature = RowSignature.builder()
                                               .addAll(inputSignature)
                                               .add("w0", ColumnType.LONG)
                                               .build();

    final WindowOperatorQuery query = new WindowOperatorQuery(
        new QueryDataSource(
            Druids.newScanQueryBuilder()
                  .dataSource(new TableDataSource("test"))
                  .intervals(new LegacySegmentSpec(Intervals.ETERNITY))
                  .columns("length")
                  .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                  .context(new HashMap<>())
                  .build()
        ),
        new LegacySegmentSpec(Intervals.ETERNITY),
        Collections.singletonMap(ResultSerializationMode.CTX_SERIALIZATION_PARAMETER, ResultSerializationMode.FRAMES.toString()),
        outputSignature,
        ImmutableList.of(
            new WindowOperatorFactory(new WindowRowNumberProcessor("w0"))
        ),
        ImmutableList.of()
    );
    List results = toolchest.mergeResults(
        (queryPlus, responseContext) -> Sequences.simple(
            Collections.singletonList(
                MapOfColumnsRowsAndColumns.fromMap(
                    ImmutableMap.of("length", new IntArrayColumn(new int[]{1, 5, 10}))
                )
            )
        )
    ).run(QueryPlus.wrap(query)).toList();

    Assert.assertTrue(results.get(0) instanceof FrameSignaturePair);
    Assert.assertEquals(1, results.size());

    FrameReader reader = FrameReader.create(((FrameSignaturePair) results.get(0)).getRowSignature());
    List<List<Object>> resultRows = FrameTestUtil.readRowsFromCursorFactory(
        reader.makeCursorFactory(((FrameSignaturePair) results.get(0)).getFrame())
    ).toList();

    List<List<Object>> expectedResults = ImmutableList.of(
        ImmutableList.of(1L, 1L),
        ImmutableList.of(5L, 2L),
        ImmutableList.of(10L, 3L)
    );
    Assertions.assertEquals(expectedResults, resultRows);
  }
}
