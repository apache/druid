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

package org.apache.druid.segment.nested;

import com.google.common.base.Preconditions;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AutoTypeColumnIndexer;
import org.apache.druid.segment.ComparatorDimensionDictionary;
import org.apache.druid.segment.ComparatorSortedDimensionDictionary;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.FrontCodedIntArrayIndexedWriter;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

/**
 * Used by {@link AutoTypeColumnIndexer} to build the value dictionary, which can be converted into a
 * {@link SortedValueDictionary} to sort and write out the values to a segment with {@link #getSortedCollector()}.
 */
public class ValueDictionary
{
  private final ComparatorDimensionDictionary<String> stringDictionary;
  private final ComparatorDimensionDictionary<Long> longDictionary;
  private final ComparatorDimensionDictionary<Double> doubleDictionary;
  private final Set<Object[]> stringArrays;
  private final Set<Object[]> longArrays;
  private final Set<Object[]> doubleArrays;

  private int arrayBytesSizeEstimate;

  public ValueDictionary()
  {
    this.stringDictionary = new ComparatorDimensionDictionary<String>(GenericIndexed.STRING_STRATEGY)
    {
      @Override
      public long estimateSizeOfValue(String value)
      {
        return StructuredDataProcessor.estimateStringSize(value);
      }
    };
    this.longDictionary = new ComparatorDimensionDictionary<Long>(ColumnType.LONG.getNullableStrategy())
    {
      @Override
      public long estimateSizeOfValue(Long value)
      {
        return StructuredDataProcessor.getLongObjectEstimateSize();
      }
    };
    this.doubleDictionary = new ComparatorDimensionDictionary<Double>(ColumnType.DOUBLE.getNullableStrategy())
    {
      @Override
      public long estimateSizeOfValue(Double value)
      {
        return StructuredDataProcessor.getDoubleObjectEstimateSize();
      }
    };
    this.stringArrays = new TreeSet<>(ColumnType.STRING_ARRAY.getNullableStrategy());
    this.longArrays = new TreeSet<>(ColumnType.LONG_ARRAY.getNullableStrategy());
    this.doubleArrays = new TreeSet<>(ColumnType.DOUBLE_ARRAY.getNullableStrategy());

    // always add default values in default value mode. they don't cost much even if they aren't used
    if (NullHandling.replaceWithDefault()) {
      longDictionary.add(NullHandling.defaultLongValue());
      doubleDictionary.add(NullHandling.defaultDoubleValue());
    }
  }

  public int addLongValue(@Nullable Long value)
  {
    longDictionary.add(value);
    return StructuredDataProcessor.getLongObjectEstimateSize();
  }

  public int addDoubleValue(@Nullable Double value)
  {
    doubleDictionary.add(value);
    return StructuredDataProcessor.getDoubleObjectEstimateSize();
  }

  public int addStringValue(@Nullable String value)
  {
    stringDictionary.add(value);
    return StructuredDataProcessor.estimateStringSize(value);
  }

  public int addStringArray(@Nullable Object[] value)
  {
    if (value == null) {
      return 0;
    }
    stringArrays.add(value);
    int sizeEstimate = 0;
    for (Object o : value) {
      if (o != null) {
        sizeEstimate += addStringValue((String) o);
      }
    }
    arrayBytesSizeEstimate += sizeEstimate;
    return sizeEstimate;
  }

  public int addLongArray(@Nullable Object[] value)
  {
    if (value == null) {
      return 0;
    }
    longArrays.add(value);
    int sizeEstimate = 0;
    for (Object o : value) {
      if (o != null) {
        sizeEstimate += addLongValue((Long) o);
      }
    }
    arrayBytesSizeEstimate += sizeEstimate;
    return sizeEstimate;
  }

  public int addDoubleArray(@Nullable Object[] value)
  {
    if (value == null) {
      return 0;
    }
    doubleArrays.add(value);
    int sizeEstimate = 0;
    for (Object o : value) {
      if (o != null) {
        sizeEstimate += addDoubleValue((Double) o);
      }
    }
    arrayBytesSizeEstimate += sizeEstimate;
    return sizeEstimate;
  }

  public SortedValueDictionary getSortedCollector()
  {
    final ComparatorSortedDimensionDictionary<String> sortedStringDimensionDictionary =
        stringDictionary.sort();

    Indexed<String> strings = new Indexed<String>()
    {
      @Override
      public int size()
      {
        return stringDictionary.size();
      }

      @Override
      public String get(int index)
      {
        return sortedStringDimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(String value)
      {
        int id = stringDictionary.getId(value);
        return id < 0
               ? DimensionDictionary.ABSENT_VALUE_ID
               : sortedStringDimensionDictionary.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<String> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };

    final ComparatorSortedDimensionDictionary<Long> sortedLongDimensionDictionary =
        longDictionary.sort();

    Indexed<Long> longs = new Indexed<Long>()
    {
      @Override
      public int size()
      {
        return longDictionary.size();
      }

      @Override
      public Long get(int index)
      {
        return sortedLongDimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(Long value)
      {
        int id = longDictionary.getId(value);
        return id < 0
               ? DimensionDictionary.ABSENT_VALUE_ID
               : sortedLongDimensionDictionary.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<Long> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };

    final ComparatorSortedDimensionDictionary<Double> sortedDoubleDimensionDictionary =
        doubleDictionary.sort();

    Indexed<Double> doubles = new Indexed<Double>()
    {
      @Override
      public int size()
      {
        return doubleDictionary.size();
      }

      @Override
      public Double get(int index)
      {
        return sortedDoubleDimensionDictionary.getValueFromSortedId(index);
      }

      @Override
      public int indexOf(Double value)
      {
        int id = doubleDictionary.getId(value);
        return id < 0
               ? DimensionDictionary.ABSENT_VALUE_ID
               : sortedDoubleDimensionDictionary.getSortedIdFromUnsortedId(id);
      }

      @Override
      public Iterator<Double> iterator()
      {
        return IndexedIterable.create(this).iterator();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };

    // offset by 1 because nulls are ignored by the indexer, but always global id 0
    final int adjustLongs = 1 + strings.size();
    final int adjustDoubles = adjustLongs + longs.size();
    TreeSet<Object[]> sortedArrays = new TreeSet<>(new Comparator<Object[]>()
    {
      @Override
      public int compare(Object[] o1, Object[] o2)
      {
        return FrontCodedIntArrayIndexedWriter.ARRAY_COMPARATOR.compare(convertArray(o1), convertArray(o2));
      }

      @Nullable
      private int[] convertArray(Object[] array)
      {
        if (array == null) {
          return null;
        }
        final int[] globalIds = new int[array.length];
        for (int i = 0; i < array.length; i++) {
          if (array[i] == null) {
            globalIds[i] = 0;
          } else if (array[i] instanceof String) {
            // offset by 1 because nulls are ignored by the indexer, but always global id 0
            globalIds[i] = 1 + strings.indexOf((String) array[i]);
          } else if (array[i] instanceof Long) {
            globalIds[i] = longs.indexOf((Long) array[i]) + adjustLongs;
          } else if (array[i] instanceof Double) {
            globalIds[i] = doubles.indexOf((Double) array[i]) + adjustDoubles;
          } else {
            globalIds[i] = -1;
          }
          Preconditions.checkArgument(
              globalIds[i] >= 0,
              "unknown global id [%s] for value [%s]",
              globalIds[i],
              array[i]
          );
        }
        return globalIds;
      }
    });
    sortedArrays.addAll(stringArrays);
    sortedArrays.addAll(longArrays);
    sortedArrays.addAll(doubleArrays);
    Indexed<Object[]> sortedArraysIndexed = new Indexed<Object[]>()
    {
      @Override
      public Iterator<Object[]> iterator()
      {
        return sortedArrays.iterator();
      }

      @Override
      public int size()
      {
        return sortedArrays.size();
      }

      @Nullable
      @Override
      public Object[] get(int index)
      {
        return new Object[0];
      }

      @Override
      public int indexOf(@Nullable Object[] value)
      {
        throw new UnsupportedOperationException("indexOf not supported");
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    };

    return new SortedValueDictionary(strings, longs, doubles, sortedArraysIndexed, null);
  }

  public long sizeInBytes()
  {
    return stringDictionary.sizeInBytes()
           + longDictionary.sizeInBytes()
           + doubleDictionary.sizeInBytes()
           + arrayBytesSizeEstimate;
  }

  public int getCardinality()
  {
    return stringDictionary.size()
           + longDictionary.size()
           + doubleDictionary.size()
           + stringArrays.size()
           + longArrays.size()
           + doubleArrays.size();
  }
}
