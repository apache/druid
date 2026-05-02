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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.GroupByQuery;

import java.util.List;

public class CompressedBigDecimalGroupByQueryConfig
{
  private final List<AggregatorFactory> ingestionAggregators;
  private final GroupByQuery query;
  private final String stringRevenue;
  private final String longRevenue;
  private final String doubleRevenue;

  public CompressedBigDecimalGroupByQueryConfig(
      List<AggregatorFactory> ingestionAggregators,
      GroupByQuery query,
      String stringRevenue,
      String longRevenue,
      String doubleRevenue
  )
  {
    this.ingestionAggregators = ingestionAggregators;
    this.query = query;
    this.stringRevenue = stringRevenue;
    this.longRevenue = longRevenue;
    this.doubleRevenue = doubleRevenue;
  }

  public List<AggregatorFactory> getIngestionAggregators()
  {
    return ingestionAggregators;
  }

  public GroupByQuery getQuery()
  {
    return query;
  }

  public String getStringRevenue()
  {
    return stringRevenue;
  }

  public String getLongRevenue()
  {
    return longRevenue;
  }

  public String getDoubleRevenue()
  {
    return doubleRevenue;
  }
}
