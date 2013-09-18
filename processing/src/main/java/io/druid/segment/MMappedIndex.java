/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment;

import com.google.common.collect.Maps;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.io.smoosh.SmooshedFileMapper;
import com.metamx.common.logger.Logger;
import io.druid.segment.data.CompressedLongsIndexedSupplier;
import io.druid.segment.data.GenericIndexed;
import io.druid.segment.data.IndexedLongs;
import io.druid.segment.data.VSizeIndexed;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Map;

/**
 */
public class MMappedIndex
{
  private static final Logger log = new Logger(MMappedIndex.class);
  private static final ImmutableConciseSet emptySet = new ImmutableConciseSet();

  final GenericIndexed<String> availableDimensions;
  final GenericIndexed<String> availableMetrics;
  final Interval dataInterval;
  final CompressedLongsIndexedSupplier timestamps;
  final Map<String, MetricHolder> metrics;
  final Map<String, GenericIndexed<String>> dimValueLookups;
  final Map<String, VSizeIndexed> dimColumns;
  final Map<String, GenericIndexed<ImmutableConciseSet>> invertedIndexes;
  final Map<String, ImmutableRTree> spatialIndexes;
  final SmooshedFileMapper fileMapper;

  private final Map<String, Integer> metricIndexes = Maps.newHashMap();

  public MMappedIndex(
      GenericIndexed<String> availableDimensions,
      GenericIndexed<String> availableMetrics,
      Interval dataInterval,
      CompressedLongsIndexedSupplier timestamps,
      Map<String, MetricHolder> metrics,
      Map<String, GenericIndexed<String>> dimValueLookups,
      Map<String, VSizeIndexed> dimColumns,
      Map<String, GenericIndexed<ImmutableConciseSet>> invertedIndexes,
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

    for (int i = 0; i < availableMetrics.size(); i++) {
      metricIndexes.put(availableMetrics.get(i), i);
    }
  }

  public CompressedLongsIndexedSupplier getTimestamps()
  {
    return timestamps;
  }

  public GenericIndexed<String> getAvailableDimensions()
  {
    return availableDimensions;
  }

  public GenericIndexed<String> getAvailableMetrics()
  {
    return availableMetrics;
  }

  public Map<String, MetricHolder> getMetrics()
  {
    return metrics;
  }

  public Integer getMetricIndex(String metricName)
  {
    return metricIndexes.get(metricName);
  }

  public Interval getDataInterval()
  {
    return dataInterval;
  }

  public IndexedLongs getReadOnlyTimestamps()
  {
    return timestamps.get();
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

  public VSizeIndexed getDimColumn(String dimension)
  {
    return dimColumns.get(dimension);
  }

  public Map<String, GenericIndexed<ImmutableConciseSet>> getInvertedIndexes()
  {
    return invertedIndexes;
  }

  public Map<String, ImmutableRTree> getSpatialIndexes()
  {
    return spatialIndexes;
  }

  public ImmutableConciseSet getInvertedIndex(String dimension, String value)
  {
    final GenericIndexed<String> lookup = dimValueLookups.get(dimension);
    if (lookup == null) {
      return emptySet;
    }

    int indexOf = lookup.indexOf(value);
    if (indexOf < 0) {
      return emptySet;
    }

    ImmutableConciseSet retVal = invertedIndexes.get(dimension).get(indexOf);
    return (retVal == null) ? emptySet : retVal;
  }

  public SmooshedFileMapper getFileMapper()
  {
    return fileMapper;
  }

  public void close() throws IOException
  {
    if (fileMapper != null) {
      fileMapper.close();
    }
  }
}
