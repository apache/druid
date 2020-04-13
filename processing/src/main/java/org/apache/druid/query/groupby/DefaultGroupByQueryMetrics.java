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

package org.apache.druid.query.groupby;

import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.DruidMetrics;

public class DefaultGroupByQueryMetrics extends DefaultQueryMetrics<GroupByQuery> implements GroupByQueryMetrics
{
  @Override
  public void query(GroupByQuery query)
  {
    super.query(query);
    numDimensions(query);
    numMetrics(query);
    numComplexMetrics(query);
    granularity(query);
  }

  @Override
  public void numDimensions(GroupByQuery query)
  {
    setDimension("numDimensions", String.valueOf(query.getDimensions().size()));
  }

  @Override
  public void numMetrics(GroupByQuery query)
  {
    setDimension("numMetrics", String.valueOf(query.getAggregatorSpecs().size()));
  }

  @Override
  public void numComplexMetrics(GroupByQuery query)
  {
    int numComplexAggs = DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs());
    setDimension("numComplexMetrics", String.valueOf(numComplexAggs));
  }

  @Override
  public void granularity(GroupByQuery query)
  {
    //Don't emit by default
  }
}
