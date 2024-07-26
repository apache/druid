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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IndexSizeExceededException;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataGenerator
{
  private final List<GeneratorColumnSchema> columnSchemas;

  private List<ColumnValueGenerator> columnGenerators;
  private final long startTime;
  private final long endTime;
  private final int numConsecutiveTimestamps;
  private final double timestampIncrement;

  private double currentTime;
  private int timeCounter;
  private List<String> dimensionNames;

  private static final Logger log = new Logger(DataGenerator.class);

  public DataGenerator(
      List<GeneratorColumnSchema> columnSchemas,
      final long seed,
      long startTime,
      int numConsecutiveTimestamps,
      Double timestampIncrement
  )
  {
    this.columnSchemas = columnSchemas;

    this.startTime = startTime;
    this.endTime = Long.MAX_VALUE;
    this.numConsecutiveTimestamps = numConsecutiveTimestamps;
    this.timestampIncrement = timestampIncrement;
    this.currentTime = startTime;

    reset(seed);
  }

  public DataGenerator(
      List<GeneratorColumnSchema> columnSchemas,
      final long seed,
      Interval interval,
      int numRows
  )
  {
    this.columnSchemas = columnSchemas;

    this.startTime = interval.getStartMillis();
    this.endTime = interval.getEndMillis() - 1;

    Preconditions.checkArgument(endTime >= startTime, "endTime >= startTime");

    long timeDelta = endTime - startTime;
    this.timestampIncrement = timeDelta / (numRows * 1.0);
    this.numConsecutiveTimestamps = 0;

    reset(seed);
  }

  public InputRow nextRow()
  {
    Map<String, Object> event = new HashMap<>();
    for (ColumnValueGenerator generator : columnGenerators) {
      event.put(generator.getSchema().getName(), generator.generateRowValue());
    }
    return new MapBasedInputRow(nextTimestamp(), dimensionNames, event);
  }

  public Map<String, Object> nextRaw()
  {
    return nextRaw(TimestampSpec.DEFAULT_COLUMN);
  }

  public Map<String, Object> nextRaw(String timestampColumn)
  {
    Map<String, Object> event = new HashMap<>();
    for (ColumnValueGenerator generator : columnGenerators) {
      event.put(generator.getSchema().getName(), generator.generateRowValue());
    }
    event.put(timestampColumn, nextTimestamp());
    return event;
  }

  /**
   * Reset this generator to start from the begining of the interval with a new seed.
   *
   * @param seed the new seed to generate rows from
   */
  public DataGenerator reset(long seed)
  {
    this.timeCounter = 0;
    this.currentTime = startTime;

    dimensionNames = new ArrayList<>();
    for (GeneratorColumnSchema schema : columnSchemas) {
      if (!schema.isMetric()) {
        dimensionNames.add(schema.getName());
      }
    }

    columnGenerators = new ArrayList<>();
    columnGenerators.addAll(
        Lists.transform(
            columnSchemas,
            new Function<GeneratorColumnSchema, ColumnValueGenerator>()
            {
              @Override
              public ColumnValueGenerator apply(
                  GeneratorColumnSchema input
              )
              {
                return input.makeGenerator(seed);
              }
            }
        )
    );

    return this;
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

  /**
   * Initialize a Java Stream generator for InputRow from this DataGenerator.
   * The generator will log its progress once every 10,000 rows.
   *
   * @param numOfRows the number of rows to generate
   * @return a generator
   */
  private Stream<InputRow> generator(int numOfRows)
  {
    return Stream.generate(
        new Supplier<InputRow>()
        {
          int i = 0;

          @Override
          public InputRow get()
          {
            InputRow row = DataGenerator.this.nextRow();
            i++;
            if (i % 10_000 == 0) {
              log.info("%,d/%,d rows generated.", i, numOfRows);
            }
            return row;
          }
        }
    ).limit(numOfRows);
  }

  /**
   * Add rows from any generator to an index.
   *
   * @param stream the stream of rows to add
   * @param index the index to add rows to
   */
  public static void addStreamToIndex(Stream<InputRow> stream, IncrementalIndex index)
  {
    stream.forEachOrdered(row -> {
      try {
        index.add(row);
      }
      catch (IndexSizeExceededException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Add rows from this generator to an index.
   *
   * @param index the index to add rows to
   * @param numOfRows the number of rows to add
   */
  public void addToIndex(IncrementalIndex index, int numOfRows)
  {
    addStreamToIndex(generator(numOfRows), index);
  }

  /**
   * Put rows from this generator to a list.
   *
   * @param numOfRows the number of rows to put in the list
   * @return a List of InputRow
   */
  public List<InputRow> toList(int numOfRows)
  {
    return generator(numOfRows).collect(Collectors.toList());
  }
}
