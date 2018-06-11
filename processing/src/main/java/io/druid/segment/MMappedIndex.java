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

package io.druid.segment;

import io.druid.collections.bitmap.ImmutableBitmap;
import io.druid.collections.spatial.ImmutableRTree;
import io.druid.java.util.common.io.smoosh.SmooshedFileMapper;
import io.druid.segment.data.CompressedColumnarLongsSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.VSizeColumnarMultiInts;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
public class MMappedIndex
{

  final GenericIndexed<String> availableDimensions;
  final GenericIndexed<String> availableMetrics;
  final Interval dataInterval;
  final CompressedColumnarLongsSupplier timestamps;
  final Map<String, MetricHolder> metrics;
  final Map<String, GenericIndexed<String>> dimValueLookups;
  final Map<String, VSizeColumnarMultiInts> dimColumns;
  final Map<String, GenericIndexed<ImmutableBitmap>> invertedIndexes;
  final Map<String, ImmutableRTree> spatialIndexes;
  final SmooshedFileMapper fileMapper;

  public MMappedIndex(
      GenericIndexed<String> availableDimensions,
      GenericIndexed<String> availableMetrics,
      Interval dataInterval,
      CompressedColumnarLongsSupplier timestamps,
      Map<String, MetricHolder> metrics,
      Map<String, GenericIndexed<String>> dimValueLookups,
      Map<String, VSizeColumnarMultiInts> dimColumns,
      Map<String, GenericIndexed<ImmutableBitmap>> invertedIndexes,
      Map<String, ImmutableRTree> spatialIndexes,
      SmooshedFileMapper fileMapper
  )
  {
    this.availableDimensions = availableDimensions;
    this.availableMetrics = availableMetrics;
    this.dataInterval = dataInterval;
    this.timestamps = timestamps;
    this.metrics = metrics;
    this.dimValueLookups = dimValueLookups;
    this.dimColumns = dimColumns;
    this.invertedIndexes = invertedIndexes;
    this.spatialIndexes = spatialIndexes;
    this.fileMapper = fileMapper;
  }

  public GenericIndexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  public GenericIndexed<String> getAvailableMetrics()
  {
    return availableMetrics;
  }

  public Interval getDataInterval()
  {
    return dataInterval;
  }

  public MetricHolder getMetricHolder(String metric)
  {
    final MetricHolder retVal = metrics.get(metric);

    if (retVal == null) {
      return null;
    }

    return retVal;
  }

  public GenericIndexed<String> getDimValueLookup(String dimension)
  {
    return dimValueLookups.get(dimension);
  }

  public VSizeColumnarMultiInts getDimColumn(String dimension)
  {
    return dimColumns.get(dimension);
  }

  public Map<String, GenericIndexed<ImmutableBitmap>> getBitmapIndexes()
  {
    return invertedIndexes;
  }

  public Map<String, ImmutableRTree> getSpatialIndexes()
  {
    return spatialIndexes;
  }

  public SmooshedFileMapper getFileMapper()
  {
    return fileMapper;
  }

}
