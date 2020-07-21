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
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReportTimelineMissingSegmentQueryRunnerTest
{
  @Test
  public void testRunWithOneSegment()
  {
    final Interval interval = Intervals.of("2020-01-01/P1D");
    final SegmentDescriptor missingSegment = new SegmentDescriptor(interval, "version", 0);
    final ReportTimelineMissingSegmentQueryRunner<Object> runner
        = new ReportTimelineMissingSegmentQueryRunner<>(missingSegment);
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    runner.run(QueryPlus.wrap(new TestQuery()), responseContext);
    Assert.assertNotNull(responseContext.get(Key.MISSING_SEGMENTS));
    Assert.assertEquals(Collections.singletonList(missingSegment), responseContext.get(Key.MISSING_SEGMENTS));
  }

  @Test
  public void testRunWithMultipleSegments()
  {
    final Interval interval = Intervals.of("2020-01-01/P1D");
    final List<SegmentDescriptor> missingSegments = ImmutableList.of(
        new SegmentDescriptor(interval, "version", 0),
        new SegmentDescriptor(interval, "version", 1)
    );
    final ReportTimelineMissingSegmentQueryRunner<Object> runner
        = new ReportTimelineMissingSegmentQueryRunner<>(missingSegments);
    final ResponseContext responseContext = DefaultResponseContext.createEmpty();
    runner.run(QueryPlus.wrap(new TestQuery()), responseContext);
    Assert.assertNotNull(responseContext.get(Key.MISSING_SEGMENTS));
    Assert.assertEquals(missingSegments, responseContext.get(Key.MISSING_SEGMENTS));
  }

  private static class TestQuery extends BaseQuery<Object>
  {
    private TestQuery()
    {
      super(
          new TableDataSource("datasource"),
          new MultipleSpecificSegmentSpec(Collections.emptyList()),
          false,
          new HashMap<>()
      );
    }

    @Override
    public boolean hasFilters()
    {
      return false;
    }

    @Override
    public DimFilter getFilter()
    {
      return null;
    }

    @Override
    public String getType()
    {
      return null;
    }

    @Override
    public Query<Object> withOverriddenContext(Map<String, Object> contextOverride)
    {
      return null;
    }

    @Override
    public Query<Object> withQuerySegmentSpec(QuerySegmentSpec spec)
    {
      return null;
    }

    @Override
    public Query<Object> withDataSource(DataSource dataSource)
    {
      return null;
    }
  }
}
