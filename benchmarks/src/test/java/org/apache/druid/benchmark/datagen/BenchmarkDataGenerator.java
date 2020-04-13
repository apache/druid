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

package org.apache.druid.benchmark.datagen;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BenchmarkDataGenerator
{
  private final List<BenchmarkColumnSchema> columnSchemas;
  private final long seed;

  private List<BenchmarkColumnValueGenerator> columnGenerators;
  private final long startTime;
  private final long endTime;
  private final int numConsecutiveTimestamps;
  private final double timestampIncrement;

  private double currentTime;
  private int timeCounter;
  private List<String> dimensionNames;

  public BenchmarkDataGenerator(
      List<BenchmarkColumnSchema> columnSchemas,
      final long seed,
      long startTime,
      int numConsecutiveTimestamps,
      Double timestampIncrement
  )
  {
    this.columnSchemas = columnSchemas;
    this.seed = seed;

    this.startTime = startTime;
    this.endTime = Long.MAX_VALUE;
    this.numConsecutiveTimestamps = numConsecutiveTimestamps;
    this.timestampIncrement = timestampIncrement;
    this.currentTime = startTime;

    init();
  }

  public BenchmarkDataGenerator(
      List<BenchmarkColumnSchema> columnSchemas,
      final long seed,
      Interval interval,
      int numRows
  )
  {
    this.columnSchemas = columnSchemas;
    this.seed = seed;

    this.startTime = interval.getStartMillis();
    this.endTime = interval.getEndMillis() - 1;

    Preconditions.checkArgument(endTime >= startTime, "endTime >= startTime");

    long timeDelta = endTime - startTime;
    this.timestampIncrement = timeDelta / (numRows * 1.0);
    this.numConsecutiveTimestamps = 0;

    init();
  }

  public InputRow nextRow()
  {
    Map<String, Object> event = new HashMap<>();
    for (BenchmarkColumnValueGenerator generator : columnGenerators) {
      event.put(generator.getSchema().getName(), generator.generateRowValue());
    }
    MapBasedInputRow row = new MapBasedInputRow(nextTimestamp(), dimensionNames, event);
    return row;
  }

  private void init()
  {
    this.timeCounter = 0;
    this.currentTime = startTime;

    dimensionNames = new ArrayList<>();
    for (BenchmarkColumnSchema schema : columnSchemas) {
      if (!schema.isMetric()) {
        dimensionNames.add(schema.getName());
      }
    }

    columnGenerators = new ArrayList<>();
    columnGenerators.addAll(
        Lists.transform(
            columnSchemas,
            new Function<BenchmarkColumnSchema, BenchmarkColumnValueGenerator>()
            {
              @Override
              public BenchmarkColumnValueGenerator apply(
                  BenchmarkColumnSchema input
              )
              {
                return input.makeGenerator(seed);
              }
            }
        )
    );
  }

  private long nextTimestamp()
  {
    timeCounter += 1;
    if (timeCounter > numConsecutiveTimestamps) {
      currentTime += timestampIncrement;
      timeCounter = 0;
    }
    long newMillis = Math.round(currentTime);
    if (newMillis > endTime) {
      return endTime;
    } else {
      return newMillis;
    }
  }

}
