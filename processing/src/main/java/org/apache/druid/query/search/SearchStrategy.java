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

package org.apache.druid.query.search;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.Druids;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.util.List;

public abstract class SearchStrategy
{
  protected final Filter filter;
  protected final Interval interval;

  protected SearchStrategy(SearchQuery query)
  {
    this.filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));
    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    if (intervals.size() != 1) {
      throw new IAE("Should only have one interval, got[%s]", intervals);
    }
    this.interval = intervals.get(0);
  }

  public abstract List<SearchQueryExecutor> getExecutionPlan(SearchQuery query, Segment segment);

  static List<DimensionSpec> getDimsToSearch(Indexed<String> availableDimensions, List<DimensionSpec> dimensions)
  {
    if (dimensions == null || dimensions.isEmpty()) {
      return ImmutableList.copyOf(Iterables.transform(availableDimensions, Druids.DIMENSION_IDENTITY));
    } else {
      return dimensions;
    }
  }
}
