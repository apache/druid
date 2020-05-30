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
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.List;

public class InlineSegmentWranglerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final InlineSegmentWrangler factory = new InlineSegmentWrangler();

  private final InlineDataSource inlineDataSource = InlineDataSource.fromIterable(
      ImmutableList.of(
          new Object[]{"foo", 1L},
          new Object[]{"bar", 2L}
      ),
      RowSignature.builder().add("str", ValueType.STRING).add("long", ValueType.LONG).build()
  );

  @Test
  public void test_getSegmentsForIntervals_nonInline()
  {
    expectedException.expect(ClassCastException.class);
    expectedException.expectMessage("TableDataSource cannot be cast");

    final Iterable<Segment> ignored = factory.getSegmentsForIntervals(
        new TableDataSource("foo"),
        Intervals.ONLY_ETERNITY
    );
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

    Assert.assertEquals(1, segments.size());

    final Segment segment = Iterables.getOnlyElement(segments);
    Assert.assertThat(segment, CoreMatchers.instanceOf(RowBasedSegment.class));
  }
}
