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

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.Cursors;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexCursorFactory;
import org.apache.druid.segment.QueryableIndexTimeBoundaryInspector;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CursorGranularizerTest extends InitializedNullHandlingTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private CursorFactory cursorFactory;
  private TimeBoundaryInspector timeBoundaryInspector;
  private Interval interval;

  @Before
  public void setup() throws IOException
  {
    final RowSignature signature = RowSignature.builder()
                                               .add("x", ColumnType.STRING)
                                               .add("y", ColumnType.STRING)
                                               .build();
    final List<String> dims = ImmutableList.of("x", "y");
    final IncrementalIndexSchema schema =
        IncrementalIndexSchema.builder()
                              .withDimensionsSpec(DimensionsSpec.builder().useSchemaDiscovery(true).build())
                              .withQueryGranularity(Granularities.MINUTE)
                              .build();
    IndexBuilder bob =
        IndexBuilder.create()
                    .schema(schema)
                    .rows(
                        ImmutableList.of(
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T00:00Z"),
                                dims,
                                ImmutableList.of("a", "1")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T00:01Z"),
                                dims,
                                ImmutableList.of("b", "2")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T00:02Z"),
                                dims,
                                ImmutableList.of("c", "1")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T00:03Z"),
                                dims,
                                ImmutableList.of("d", "2")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T01:00Z"),
                                dims,
                                ImmutableList.of("e", "1")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T01:01Z"),
                                dims,
                                ImmutableList.of("f", "2")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T03:04Z"),
                                dims,
                                ImmutableList.of("g", "1")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T03:05Z"),
                                dims,
                                ImmutableList.of("h", "2")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T03:15Z"),
                                dims,
                                ImmutableList.of("i", "1")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T05:03Z"),
                                dims,
                                ImmutableList.of("j", "2")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T06:00Z"),
                                dims,
                                ImmutableList.of("k", "1")
                            ),
                            new ListBasedInputRow(
                                signature,
                                DateTimes.of("2024-01-01T09:01Z"),
                                dims,
                                ImmutableList.of("l", "2")
                            )
                        )
                    )
                    .tmpDir(temporaryFolder.newFolder());

    final QueryableIndex index = bob.buildMMappedIndex(Intervals.of("2024-01-01T00:00Z/2024-01-02T00:00Z"));
    interval = index.getDataInterval();
    cursorFactory = new QueryableIndexCursorFactory(index);
    timeBoundaryInspector = QueryableIndexTimeBoundaryInspector.create(index);
  }

  @Test
  public void testGranularizeFullScan()
  {
    try (CursorHolder cursorHolder = cursorFactory.makeCursorHolder(CursorBuildSpec.FULL_SCAN)) {
      final Cursor cursor = cursorHolder.asCursor();
      CursorGranularizer granularizer = CursorGranularizer.create(
          cursor,
          timeBoundaryInspector,
          Order.ASCENDING,
          Granularities.HOUR,
          interval
      );

      final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();
      final ColumnValueSelector xSelector = selectorFactory.makeColumnValueSelector("x");
      final Sequence<List<String>> theSequence =
          Sequences.simple(granularizer.getBucketIterable())
                   .map(bucketInterval -> {
                     List<String> bucket = new ArrayList<>();
                     if (!granularizer.advanceToBucket(bucketInterval)) {
                       return bucket;
                     }
                     while (!cursor.isDone()) {
                       bucket.add((String) xSelector.getObject());
                       if (!granularizer.advanceCursorWithinBucket()) {
                         break;
                       }
                     }
                     return bucket;
                   });

      List<List<String>> granularized = theSequence.toList();
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", "b", "c", "d"),
              ImmutableList.of("e", "f"),
              ImmutableList.of(),
              ImmutableList.of("g", "h", "i"),
              ImmutableList.of(),
              ImmutableList.of("j"),
              ImmutableList.of("k"),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of("l")
          ),
          granularized
      );
    }
  }

  @Test
  public void testGranularizeFullScanDescending()
  {
    final CursorBuildSpec descending = CursorBuildSpec.builder()
                                                      .setPreferredOrdering(Cursors.descendingTimeOrder())
                                                      .build();
    try (CursorHolder cursorHolder = cursorFactory.makeCursorHolder(descending)) {
      final Cursor cursor = cursorHolder.asCursor();
      CursorGranularizer granularizer = CursorGranularizer.create(
          cursor,
          timeBoundaryInspector,
          Order.DESCENDING,
          Granularities.HOUR,
          interval
      );

      final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();
      final ColumnValueSelector xSelector = selectorFactory.makeColumnValueSelector("x");
      final Sequence<List<String>> theSequence =
          Sequences.simple(granularizer.getBucketIterable())
                   .map(bucketInterval -> {
                     List<String> bucket = new ArrayList<>();
                     if (!granularizer.advanceToBucket(bucketInterval)) {
                       return bucket;
                     }
                     while (!cursor.isDone()) {
                       bucket.add((String) xSelector.getObject());
                       if (!granularizer.advanceCursorWithinBucket()) {
                         break;
                       }
                     }
                     return bucket;
                   });

      List<List<String>> granularized = theSequence.toList();
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableList.of("l"),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of("k"),
              ImmutableList.of("j"),
              ImmutableList.of(),
              ImmutableList.of("i", "h", "g"),
              ImmutableList.of(),
              ImmutableList.of("f", "e"),
              ImmutableList.of("d", "c", "b", "a")
          ),
          granularized
      );
    }
  }

  @Test
  public void testGranularizeFiltered()
  {
    final CursorBuildSpec filtered = CursorBuildSpec.builder()
                                                    .setFilter(new EqualityFilter("y", ColumnType.STRING, "1", null))
                                                    .build();
    try (CursorHolder cursorHolder = cursorFactory.makeCursorHolder(filtered)) {
      final Cursor cursor = cursorHolder.asCursor();
      CursorGranularizer granularizer = CursorGranularizer.create(
          cursor,
          timeBoundaryInspector,
          Order.ASCENDING,
          Granularities.HOUR,
          interval
      );

      final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();
      final ColumnValueSelector xSelector = selectorFactory.makeColumnValueSelector("x");
      final Sequence<List<String>> theSequence =
          Sequences.simple(granularizer.getBucketIterable())
                   .map(bucketInterval -> {
                     List<String> bucket = new ArrayList<>();
                     if (!granularizer.advanceToBucket(bucketInterval)) {
                       return bucket;
                     }
                     while (!cursor.isDone()) {
                       bucket.add((String) xSelector.getObject());
                       if (!granularizer.advanceCursorWithinBucket()) {
                         break;
                       }
                     }
                     return bucket;
                   });

      List<List<String>> granularized = theSequence.toList();
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableList.of("a", "c"),
              ImmutableList.of("e"),
              ImmutableList.of(),
              ImmutableList.of("g", "i"),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of("k"),
              ImmutableList.of(),
              ImmutableList.of(),
              ImmutableList.of()
          ),
          granularized
      );
    }
  }

  @Test
  public void testGranularizeFilteredClippedAndPartialOverlap()
  {
    final CursorBuildSpec filtered = CursorBuildSpec.builder()
                                                    .setFilter(new EqualityFilter("y", ColumnType.STRING, "1", null))
                                                    .build();
    try (CursorHolder cursorHolder = cursorFactory.makeCursorHolder(filtered)) {
      final Cursor cursor = cursorHolder.asCursor();
      CursorGranularizer granularizer = CursorGranularizer.create(
          cursor,
          timeBoundaryInspector,
          Order.ASCENDING,
          Granularities.HOUR,
          Intervals.of("2024-01-01T08:00Z/2024-01-03T00:00Z")
      );

      final ColumnSelectorFactory selectorFactory = cursor.getColumnSelectorFactory();
      final ColumnValueSelector xSelector = selectorFactory.makeColumnValueSelector("x");
      final Sequence<List<String>> theSequence =
          Sequences.simple(granularizer.getBucketIterable())
                   .map(bucketInterval -> {
                     List<String> bucket = new ArrayList<>();
                     if (!granularizer.advanceToBucket(bucketInterval)) {
                       return bucket;
                     }
                     while (!cursor.isDone()) {
                       bucket.add((String) xSelector.getObject());
                       if (!granularizer.advanceCursorWithinBucket()) {
                         break;
                       }
                     }
                     return bucket;
                   });

      List<List<String>> granularized = theSequence.toList();
      Assert.assertEquals(
          ImmutableList.of(
              ImmutableList.of(),
              ImmutableList.of()
          ),
          granularized
      );
    }
  }
}
