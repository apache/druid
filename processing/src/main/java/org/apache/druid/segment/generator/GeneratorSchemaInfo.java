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

package org.apache.druid.segment.generator;

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.joda.time.Interval;

import java.util.List;
import java.util.stream.Collectors;

public class GeneratorSchemaInfo
{
  private List<GeneratorColumnSchema> columnSchemas;
  private List<AggregatorFactory> aggs;
  private Interval dataInterval;
  private boolean withRollup;

  public GeneratorSchemaInfo(
      List<GeneratorColumnSchema> columnSchemas,
      List<AggregatorFactory> aggs,
      Interval dataInterval,
      boolean withRollup
  )
  {
    this.columnSchemas = columnSchemas;
    this.aggs = aggs;
    this.dataInterval = dataInterval;
    this.withRollup = withRollup;
  }

  public List<GeneratorColumnSchema> getColumnSchemas()
  {
    return columnSchemas;
  }

  public DimensionsSpec getDimensionsSpec()
  {
    List<DimensionSchema> specs = getColumnSchemas().stream()
                                                    .filter(x -> !x.isMetric())
                                                    .map(GeneratorColumnSchema::getDimensionSchema)
                                                    .collect(Collectors.toList());

    return new DimensionsSpec(specs);
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

  public boolean isWithRollup()
  {
    return withRollup;
  }

  public DataSegment makeSegmentDescriptor(final String dataSource)
  {
    return DataSegment.builder()
                      .dataSource(dataSource)
                      .interval(getDataInterval())
                      .version("1")
                      .shardSpec(new LinearShardSpec(0))
                      .size(0)
                      .build();
  }

  @Override
  public String toString()
  {
    return "BenchmarkSchemaInfo{" +
           "columnSchemas=" + columnSchemas +
           ", aggs=" + aggs +
           ", dataInterval=" + dataInterval +
           ", withRollup=" + withRollup +
           '}';
  }
}
