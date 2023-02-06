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
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class FrameComparisonWidgetImplTest extends InitializedNullHandlingTest
{
  private Frame frame;

  @Before
  public void setUp()
  {
    final StorageAdapter rowBasedAdapter = new RowBasedSegment<>(
        SegmentId.dummy("test"),
        Sequences.simple(RowKeyComparatorTest.ALL_KEY_OBJECTS),
        columnName -> {
          final int idx = RowKeyComparatorTest.SIGNATURE.getColumnNames().indexOf(columnName);
          if (idx < 0) {
            return row -> null;
          } else {
            return row -> row[idx];
          }
        },
        RowKeyComparatorTest.SIGNATURE
    ).asStorageAdapter();

    frame = Iterables.getOnlyElement(
        FrameSequenceBuilder.fromAdapter(rowBasedAdapter)
                            .frameType(FrameType.ROW_BASED)
                            .frames()
                            .toList()
    );
  }

  @Test
  public void test_readKey_someColumns()
  {
    final List<SortColumn> sortColumns = ImmutableList.of(
        new SortColumn("1", false),
        new SortColumn("2", false),
        new SortColumn("3", false)
    );

    final FrameComparisonWidget widget =
        FrameComparisonWidgetImpl.create(frame, FrameReader.create(RowKeyComparatorTest.SIGNATURE), sortColumns);

    final RowSignature signature =
        RowSignature.builder()
                    .add("1", RowKeyComparatorTest.SIGNATURE.getColumnType("1").orElse(null))
                    .add("2", RowKeyComparatorTest.SIGNATURE.getColumnType("2").orElse(null))
                    .add("3", RowKeyComparatorTest.SIGNATURE.getColumnType("3").orElse(null))
                    .build();

    for (int i = 0; i < frame.numRows(); i++) {
      final Object[] expectedKeyArray = new Object[sortColumns.size()];
      System.arraycopy(RowKeyComparatorTest.ALL_KEY_OBJECTS.get(i), 0, expectedKeyArray, 0, sortColumns.size());
      Assert.assertEquals(
          KeyTestUtils.createKey(signature, expectedKeyArray),
          widget.readKey(i)
      );
    }
  }

  @Test
  public void test_readKey_allColumns()
  {
    final List<SortColumn> sortColumns = ImmutableList.of(
        new SortColumn("1", false),
        new SortColumn("2", false),
        new SortColumn("3", false),
        new SortColumn("4", false)
    );

    final FrameComparisonWidget widget =
        FrameComparisonWidgetImpl.create(frame, FrameReader.create(RowKeyComparatorTest.SIGNATURE), sortColumns);

    for (int i = 0; i < frame.numRows(); i++) {
      Assert.assertEquals(
          KeyTestUtils.createKey(RowKeyComparatorTest.SIGNATURE, RowKeyComparatorTest.ALL_KEY_OBJECTS.get(i)),
          widget.readKey(i)
      );
    }
  }

  @Test
  public void test_compare_frameToKey()
  {
    final List<SortColumn> sortColumns = ImmutableList.of(
        new SortColumn("1", false),
        new SortColumn("2", false),
        new SortColumn("3", false),
        new SortColumn("4", false)
    );

    final FrameComparisonWidget widget =
        FrameComparisonWidgetImpl.create(frame, FrameReader.create(RowKeyComparatorTest.SIGNATURE), sortColumns);

    // Compare self-to-self should be equal.
    for (int i = 0; i < frame.numRows(); i++) {
      Assert.assertEquals(
          0,
          widget.compare(
              i,
              KeyTestUtils.createKey(
                  RowKeyComparatorTest.SIGNATURE,
                  RowKeyComparatorTest.ALL_KEY_OBJECTS.get(i)
              )
          )
      );
    }

    // Check some other comparators.
    final RowKey firstKey = KeyTestUtils.createKey(
        RowKeyComparatorTest.SIGNATURE,
        RowKeyComparatorTest.ALL_KEY_OBJECTS.get(0)
    );

    MatcherAssert.assertThat(widget.compare(0, firstKey), Matchers.equalTo(0));
    MatcherAssert.assertThat(widget.compare(1, firstKey), Matchers.lessThan(0));
    MatcherAssert.assertThat(widget.compare(2, firstKey), Matchers.lessThan(0));
    MatcherAssert.assertThat(widget.compare(3, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(4, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(5, firstKey), Matchers.greaterThan(0));
    MatcherAssert.assertThat(widget.compare(6, firstKey), Matchers.greaterThan(0));
  }
}
