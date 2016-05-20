/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.datagen;

import io.druid.benchmark.datagen.BenchmarkColumnSchema;
import io.druid.query.aggregation.AggregatorFactory;
import org.joda.time.Interval;

import java.util.List;

public class BenchmarkSchemaInfo
{
  private List<BenchmarkColumnSchema> columnSchemas;
  private List<AggregatorFactory> aggs;
  private Interval dataInterval;

  public BenchmarkSchemaInfo (
      List<BenchmarkColumnSchema> columnSchemas,
      List<AggregatorFactory> aggs,
      Interval dataInterval
  )
  {
    this.columnSchemas = columnSchemas;
    this.aggs = aggs;
    this.dataInterval = dataInterval;
  }

  public List<BenchmarkColumnSchema> getColumnSchemas()
  {
    return columnSchemas;
  }

  public List<AggregatorFactory> getAggs()
  {
    return aggs;
  }

  public AggregatorFactory[] getAggsArray()
  {
    return aggs.toArray(new AggregatorFactory[0]);
  }

  public Interval getDataInterval()
  {
    return dataInterval;
  }

}
