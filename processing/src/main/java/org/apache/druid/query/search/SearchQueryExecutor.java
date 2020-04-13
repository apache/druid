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

import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.Segment;

import java.util.List;

public abstract class SearchQueryExecutor
{
  protected final SearchQuery query;
  protected final SearchQuerySpec searchQuerySpec;
  protected final Segment segment;
  protected final List<DimensionSpec> dimsToSearch;

  public SearchQueryExecutor(SearchQuery query, Segment segment, List<DimensionSpec> dimensionSpecs)
  {
    this.query = query;
    this.segment = segment;
    this.searchQuerySpec = query.getQuery();
    this.dimsToSearch = dimensionSpecs;
  }

  public abstract Object2IntRBTreeMap<SearchHit> execute(int limit);
}
