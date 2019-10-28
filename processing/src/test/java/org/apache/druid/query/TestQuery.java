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

import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.spec.QuerySegmentSpec;

import java.util.Map;

public class TestQuery extends BaseQuery
{

  public TestQuery(DataSource dataSource, QuerySegmentSpec querySegmentSpec, boolean descending, Map context)
  {
    super(dataSource, querySegmentSpec, descending, context);
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
  public Query withQuerySegmentSpec(QuerySegmentSpec spec)
  {
    return null;
  }

  @Override
  public Query withDataSource(DataSource dataSource)
  {
    return null;
  }

  @Override
  public Query withOverriddenContext(Map contextOverride)
  {
    return new TestQuery(
        getDataSource(),
        getQuerySegmentSpec(),
        isDescending(),
        BaseQuery.computeOverriddenContext(getContext(), contextOverride)
    );
  }
}
