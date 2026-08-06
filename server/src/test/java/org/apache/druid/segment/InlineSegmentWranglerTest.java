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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InlineSegmentWranglerTest
{

  private final InlineSegmentWrangler factory = new InlineSegmentWrangler();

  private final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"foo", 1L},
          new Object[]{"bar", 2L}
      ),
      RowSignature.builder().add("str", ColumnType.STRING).add("long", ColumnType.LONG).build()
  );

  @Test
  public void test_getSegmentsForIntervals_nonInline()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(ClassCastException.class, () -> {

      final Iterable<Segment> ignored = factory.getSegmentsForIntervals(
          new TableDataSource("foo"),
          Intervals.ONLY_ETERNITY
      );
    });
    assertTrue(exception.getMessage().contains("TableDataSource cannot be cast"));
  }

  @Test
  public void test_getSegmentsForIntervals_inline()
  {
    final List<Segment> segments = ImmutableList.copyOf(
        factory.getSegmentsForIntervals(
            inlineDataSource,
            Intervals.ONLY_ETERNITY
        )
    );

    Assertions.assertEquals(1, segments.size());

    final Segment segment = Iterables.getOnlyElement(segments);
    assertThat(segment).isInstanceOf(RowBasedSegment.class);
  }
}
