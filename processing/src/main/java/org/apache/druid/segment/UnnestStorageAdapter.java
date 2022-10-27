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

package org.apache.druid.segment;

import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;

public class UnnestStorageAdapter implements StorageAdapter
{
  private final StorageAdapter baseAdapter;
  private final String dimensionToUnnest;
  private final boolean dedup;
  private final String outputColumnName;

  public UnnestStorageAdapter(
      final StorageAdapter baseAdapter,
      final String dimension,
      final String outputColumnName,
      final boolean dedup
  )
  {
    this.baseAdapter = baseAdapter;
    this.dimensionToUnnest = dimension;
    this.outputColumnName = outputColumnName;
    this.dedup = dedup;
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable Filter filter,
      Interval interval,
      VirtualColumns virtualColumns,
      Granularity gran,
      boolean descending,
      @Nullable QueryMetrics<?> queryMetrics
  )
  {
    final Sequence<Cursor> baseCursorSequence = baseAdapter.makeCursors(
        filter,
        interval,
        virtualColumns,
        gran,
        descending,
        queryMetrics
    );
    return Sequences.map(
        baseCursorSequence,
        cursor -> {
          assert cursor != null;
          Cursor retVal = cursor;
          retVal = new UnnestCursor(retVal, dimensionToUnnest, outputColumnName, dedup);
          return retVal;
        }
    );
  }

  @Override
  public Interval getInterval()
  {
    return baseAdapter.getInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    final LinkedHashSet<String> availableDimensions = new LinkedHashSet<>();

    for (String dim : baseAdapter.getAvailableDimensions()) {
      availableDimensions.add(dim);
    }
    // check to see if output name provided is already
    // a part of available dimensions
    if (availableDimensions.contains(outputColumnName)) {
      throw new IAE(
          "Provided output name [%s] already exists in table to be unnested. Please use a different name.",
          outputColumnName
      );
    } else {
      availableDimensions.add(outputColumnName);
    }
    return new ListIndexed<>(Lists.newArrayList(availableDimensions));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return baseAdapter.getAvailableMetrics();
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    //intercept
    return baseAdapter.getDimensionCardinality(column);
  }

  @Override
  public DateTime getMinTime()
  {
    return baseAdapter.getMinTime();
  }

  @Override
  public DateTime getMaxTime()
  {
    return baseAdapter.getMaxTime();
  }

  @Nullable
  @Override
  public Comparable getMinValue(String column)
  {
    //intercept
    return baseAdapter.getMinValue(column);
  }

  @Nullable
  @Override
  public Comparable getMaxValue(String column)
  {
    //intercept
    return baseAdapter.getMaxValue(column);
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return baseAdapter.getColumnCapabilities(column);
  }

  @Override
  public int getNumRows()
  {
    return 0;
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return baseAdapter.getMaxIngestedEventTime();
  }

  @Nullable
  @Override
  public Metadata getMetadata()
  {
    return baseAdapter.getMetadata();
  }

  public String getDimensionToUnnest()
  {
    return dimensionToUnnest;
  }
}

