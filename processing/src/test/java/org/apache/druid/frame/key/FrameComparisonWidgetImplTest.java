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

package org.apache.druid.frame.key;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.field.FieldReaders;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FrameComparisonWidgetImplTest extends InitializedNullHandlingTest
{
  private Frame frameWithoutComplexColumns;
  private Frame frameWithComplexColumns;

  @Before
  public void setUp()
  {
    final CursorFactory rowBasedAdapterWithoutComplexColumn = new RowBasedSegment<>(
        SegmentId.dummy("test"),
        Sequences.simple(ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN),
        columnName -> {
          final int idx = ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE.getColumnNames().indexOf(columnName);
          if (idx < 0) {
            return row -> null;
          } else {
            return row -> row[idx];
          }
        },
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE
    ).asCursorFactory();

    frameWithoutComplexColumns = Iterables.getOnlyElement(
        FrameSequenceBuilder.fromCursorFactory(rowBasedAdapterWithoutComplexColumn)
                            .frameType(FrameType.ROW_BASED)
                            .frames()
                            .toList()
    );

    final CursorFactory rowBasedAdapterWithComplexColumn = new RowBasedSegment<>(
        SegmentId.dummy("test"),
        Sequences.simple(ByteRowKeyComparatorTest.ALL_KEY_OBJECTS),
        columnName -> {
          final int idx = ByteRowKeyComparatorTest.SIGNATURE.getColumnNames().indexOf(columnName);
          if (idx < 0) {
            return row -> null;
          } else {
            return row -> row[idx];
          }
        },
        ByteRowKeyComparatorTest.SIGNATURE
    ).asCursorFactory();

    frameWithComplexColumns = Iterables.getOnlyElement(
        FrameSequenceBuilder.fromCursorFactory(rowBasedAdapterWithComplexColumn)
                            .frameType(FrameType.ROW_BASED)
                            .frames()
                            .toList()
    );
  }

  @Test
  public void test_noComplexColumns_isPartiallyNullKey_someColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithoutComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE
    );

    for (int i = 0; i < frameWithoutComplexColumns.numRows(); i++) {
      final boolean isAllNonNull =
          Arrays.stream(ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN.get(i))
                .limit(3)
                .allMatch(Objects::nonNull);

      // null key part, if any, is always the second one (1)
      Assert.assertTrue(widget.hasNonNullKeyParts(i, new int[0]));
      Assert.assertTrue(widget.hasNonNullKeyParts(i, new int[]{0, 2}));
      Assert.assertEquals(isAllNonNull, widget.hasNonNullKeyParts(i, new int[]{0, 1, 2}));
      Assert.assertEquals(isAllNonNull, widget.hasNonNullKeyParts(i, new int[]{1}));
    }
  }

  @Test
  public void test_noComplexColumns_isPartiallyNullKey_allColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithoutComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE
    );

    for (int i = 0; i < frameWithoutComplexColumns.numRows(); i++) {
      final boolean isAllNonNull =
          Arrays.stream(ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN.get(i)).allMatch(Objects::nonNull);
      Assert.assertEquals(isAllNonNull, widget.hasNonNullKeyParts(i, new int[]{0, 1, 2, 3}));
    }
  }

  @Test
  public void test_noComplexColumns_readKey_someColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithoutComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE
    );

    final RowSignature signature =
        RowSignature.builder()
                    .add("1", ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE.getColumnType("1").orElse(null))
                    .add("2", ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE.getColumnType("2").orElse(null))
                    .add("3", ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE.getColumnType("3").orElse(null))
                    .build();

    for (int i = 0; i < frameWithoutComplexColumns.numRows(); i++) {
      final Object[] expectedKeyArray = new Object[keyColumns.size()];
      System.arraycopy(
          ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN.get(i),
          0,
          expectedKeyArray,
          0,
          keyColumns.size()
      );
      Assert.assertEquals(
          KeyTestUtils.createKey(signature, expectedKeyArray),
          widget.readKey(i)
      );
    }
  }

  @Test
  public void test_noComplexColumns_readKey_allColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithoutComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE
    );

    for (int i = 0; i < frameWithoutComplexColumns.numRows(); i++) {
      Assert.assertEquals(
          KeyTestUtils.createKey(
              ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE,
              ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN.get(i)
          ),
          widget.readKey(i)
      );
    }
  }

  @Test
  public void test_noComplexColumns_compare_frameToKey()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithoutComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE
    );

    // Compare self-to-self should be equal.
    for (int i = 0; i < frameWithoutComplexColumns.numRows(); i++) {
      Assert.assertEquals(
          0,
          widget.compare(
              i,
              KeyTestUtils.createKey(
                  ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE,
                  ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN.get(i)
              )
          )
      );
    }

    // Check some other comparators.
    final RowKey firstKey = KeyTestUtils.createKey(
        ByteRowKeyComparatorTest.NO_COMPLEX_SIGNATURE,
        ByteRowKeyComparatorTest.KEY_OBJECTS_WITHOUT_COMPLEX_COLUMN.get(0)
    );

    MatcherAssert.assertThat(widget.compare(0, firstKey), Matchers.equalTo(0));
    MatcherAssert.assertThat(widget.compare(1, firstKey), Matchers.lessThan(0));
    MatcherAssert.assertThat(widget.compare(2, firstKey), Matchers.lessThan(0));
    MatcherAssert.assertThat(widget.compare(3, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(4, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(5, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(6, firstKey), Matchers.greaterThan(0));
  }

  @Test
  public void test_isPartiallyNullKey_someColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.SIGNATURE
    );

    for (int i = 0; i < frameWithComplexColumns.numRows(); i++) {
      final boolean isAllNonNull =
          Arrays.stream(ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(i)).limit(3).allMatch(Objects::nonNull);

      // Only second is non-null throughout
      Assert.assertTrue(widget.hasNonNullKeyParts(i, new int[]{1}));
      Assert.assertTrue(widget.hasNonNullKeyParts(i, new int[]{1}));
      Assert.assertEquals(isAllNonNull, widget.hasNonNullKeyParts(i, new int[]{0, 1, 2}));
      Assert.assertEquals(
          isAllNonNull,
          widget.hasNonNullKeyParts(i, new int[]{0}) && widget.hasNonNullKeyParts(i, new int[]{2})
      );
    }
  }

  @Test
  public void test_isPartiallyNullKey_allColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING),
        new KeyColumn("7", KeyOrder.ASCENDING),
        new KeyColumn("8", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.SIGNATURE
    );

    for (int i = 0; i < frameWithoutComplexColumns.numRows(); i++) {
      final boolean isAllNonNull =
          Arrays.stream(ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(i)).allMatch(Objects::nonNull);
      Assert.assertEquals(isAllNonNull, widget.hasNonNullKeyParts(i, new int[]{0, 1, 2, 3, 4, 5, 6, 7}));
    }
  }

  @Test
  public void test_readKey_someColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.SIGNATURE
    );

    final RowSignature signature =
        RowSignature.builder()
                    .add("1", ByteRowKeyComparatorTest.SIGNATURE.getColumnType("1").orElse(null))
                    .add("2", ByteRowKeyComparatorTest.SIGNATURE.getColumnType("2").orElse(null))
                    .add("3", ByteRowKeyComparatorTest.SIGNATURE.getColumnType("3").orElse(null))
                    .build();

    for (int i = 0; i < frameWithComplexColumns.numRows(); i++) {
      final Object[] expectedKeyArray = new Object[keyColumns.size()];
      System.arraycopy(ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(i), 0, expectedKeyArray, 0, keyColumns.size());
      Assert.assertEquals(
          KeyTestUtils.createKey(signature, expectedKeyArray),
          widget.readKey(i)
      );
    }
  }

  @Test
  public void test_readKey_allColumns()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING),
        new KeyColumn("7", KeyOrder.ASCENDING),
        new KeyColumn("8", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.SIGNATURE
    );

    for (int i = 0; i < frameWithComplexColumns.numRows(); i++) {
      Assert.assertEquals(
          KeyTestUtils.createKey(ByteRowKeyComparatorTest.SIGNATURE, ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(i)),
          widget.readKey(i)
      );
    }
  }

  @Test
  public void test_compare_frameToKey()
  {
    final List<KeyColumn> keyColumns = ImmutableList.of(
        new KeyColumn("1", KeyOrder.ASCENDING),
        new KeyColumn("2", KeyOrder.ASCENDING),
        new KeyColumn("3", KeyOrder.ASCENDING),
        new KeyColumn("4", KeyOrder.ASCENDING),
        new KeyColumn("5", KeyOrder.ASCENDING),
        new KeyColumn("6", KeyOrder.ASCENDING),
        new KeyColumn("7", KeyOrder.ASCENDING),
        new KeyColumn("8", KeyOrder.ASCENDING)
    );

    final FrameComparisonWidget widget = createComparisonWidget(
        frameWithComplexColumns,
        keyColumns,
        ByteRowKeyComparatorTest.SIGNATURE
    );

    // Compare self-to-self should be equal.
    for (int i = 0; i < frameWithComplexColumns.numRows(); i++) {
      Assert.assertEquals(
          0,
          widget.compare(
              i,
              KeyTestUtils.createKey(
                  ByteRowKeyComparatorTest.SIGNATURE,
                  ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(i)
              )
          )
      );
    }

    // Check some other comparators.
    final RowKey firstKey = KeyTestUtils.createKey(
        ByteRowKeyComparatorTest.SIGNATURE,
        ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(0)
    );

    MatcherAssert.assertThat(widget.compare(0, firstKey), Matchers.equalTo(0));
    MatcherAssert.assertThat(widget.compare(1, firstKey), Matchers.lessThan(0));
    MatcherAssert.assertThat(widget.compare(2, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(3, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(4, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(5, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(6, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(7, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(8, firstKey), Matchers.greaterThan(0));

    final RowKey eighthKey = KeyTestUtils.createKey(
        ByteRowKeyComparatorTest.SIGNATURE,
        ByteRowKeyComparatorTest.ALL_KEY_OBJECTS.get(7)
    );

    MatcherAssert.assertThat(widget.compare(8, eighthKey), Matchers.lessThan(0));
  }

  private FrameComparisonWidget createComparisonWidget(
      final Frame frame,
      final List<KeyColumn> keyColumns,
      final RowSignature rowSignature
  )
  {
    return FrameComparisonWidgetImpl.create(
        frame,
        rowSignature,
        keyColumns,
        keyColumns.stream().map(
            keyColumn ->
                FieldReaders.create(
                    keyColumn.columnName(),
                    rowSignature.getColumnType(keyColumn.columnName()).get()
                )
        ).collect(Collectors.toList())
    );
  }
}
