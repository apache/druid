/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.metamx.collections.spatial.ImmutableRTree;
import com.metamx.common.logger.Logger;
import com.metamx.druid.kv.ConciseCompressedIndexedInts;
import com.metamx.druid.kv.GenericIndexed;
import com.metamx.druid.kv.Indexed;
import com.metamx.druid.kv.IndexedList;
import com.metamx.druid.kv.IndexedLongs;
import com.metamx.druid.kv.IndexedRTree;
import com.metamx.druid.kv.VSizeIndexed;
import com.metamx.druid.kv.VSizeIndexedInts;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import org.joda.time.Interval;

import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.util.Arrays;
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
      Map<String, ImmutableRTree> spatialIndexes
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

  public static MMappedIndex fromIndex(Index index)
  {
    log.info("Converting timestamps");
    CompressedLongsIndexedSupplier timestamps = CompressedLongsIndexedSupplier.fromLongBuffer(
        LongBuffer.wrap(index.timeOffsets), ByteOrder.nativeOrder()
    );

    log.info("Converting dimValueLookups");
    Map<String, GenericIndexed<String>> dimValueLookups = Maps.newHashMap();
    for (Map.Entry<String, String[]> entry : index.reverseDimLookup.entrySet()) {
      String[] theValues = Arrays.copyOf(entry.getValue(), entry.getValue().length);
      Arrays.sort(theValues);

      dimValueLookups.put(entry.getKey(), GenericIndexed.fromArray(theValues, GenericIndexed.stringStrategy));
    }

    Map<String, VSizeIndexed> dimColumns = Maps.newHashMap();
    Map<String, GenericIndexed<ImmutableConciseSet>> invertedIndexes = Maps.newLinkedHashMap();
    Map<String, ImmutableRTree> spatialIndexes = Maps.newLinkedHashMap();

    for (String dimension : Arrays.asList(index.dimensions)) {
      final String[] dimVals = index.reverseDimLookup.get(dimension);
      final DimensionColumn dimColumn = index.dimensionValues.get(dimension);

      log.info(
          "Converting dim[%s] with valueCardinality[%,d] and expansionCardinality[%,d]",
          dimension,
          dimColumn.getDimensionExpansions().length,
          dimVals.length
      );

      final Indexed<String> lookup = dimValueLookups.get(dimension);
      final int[] expansionConversion = new int[dimVals.length];
      final int[] reverseExpansionConversion = new int[dimVals.length];
      for (int i = 0; i < expansionConversion.length; i++) {
        expansionConversion[i] = lookup.indexOf(dimVals[i]);
        reverseExpansionConversion[expansionConversion[i]] = i;
      }

      int[][] originalDimExpansions = dimColumn.getDimensionExpansions();
      final int[][] dimensionExpansions = new int[originalDimExpansions.length][];
      for (int i = 0; i < originalDimExpansions.length; i++) {
        int[] originalDimExpansion = originalDimExpansions[i];
        int[] mappedValues = new int[originalDimExpansion.length];

        for (int j = 0; j < originalDimExpansion.length; j++) {
          mappedValues[j] = expansionConversion[originalDimExpansion[j]];
        }
        Arrays.sort(mappedValues);

        dimensionExpansions[i] = mappedValues;
      }

      final int[] originalRows = dimColumn.getDimensionRowValues();
      final ImmutableConciseSet[] origInvertedIndexes = index.indexes.get(dimension);

      dimColumns.put(
          dimension,
          VSizeIndexed.fromIterable(
              Iterables.transform(
                  Ints.asList(originalRows),
                  new Function<Integer, VSizeIndexedInts>()
                  {
                    @Override
                    public VSizeIndexedInts apply(Integer input)
                    {
                      return VSizeIndexedInts.fromArray(dimensionExpansions[input], dimVals.length - 1);
                    }
                  }
              )
          )
      );

      invertedIndexes.put(
          dimension,
          GenericIndexed.fromIterable(
              Iterables.transform(
                  new IndexedList<String>(lookup),
                  new Function<String, ImmutableConciseSet>()
                  {
                    @Override
                    public ImmutableConciseSet apply(String input)
                    {
                      return origInvertedIndexes[reverseExpansionConversion[lookup.indexOf(input)]];
                    }
                  }
              ),
              ConciseCompressedIndexedInts.objectStrategy
          )
      );

      spatialIndexes.put(dimension, index.getSpatialIndex(dimension));
    }

    log.info("Making MMappedIndex");
    return new MMappedIndex(
        GenericIndexed.fromArray(index.dimensions, GenericIndexed.stringStrategy),
        GenericIndexed.fromArray(index.metrics, GenericIndexed.stringStrategy),
        index.dataInterval,
        timestamps,
        index.metricVals,
        dimValueLookups,
        dimColumns,
        invertedIndexes,
        spatialIndexes
    );
  }
}
