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

import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.inject.ImplementedBy;
import io.druid.common.utils.SerializerUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

@ImplementedBy(IndexMergerV9.class)
public interface IndexMerger
{
  Logger log = new Logger(IndexMerger.class);

  SerializerUtils serializerUtils = new SerializerUtils();
  int INVALID_ROW = -1;

  static List<String> getMergedDimensionsFromQueryableIndexes(List<QueryableIndex> indexes)
  {
    return getMergedDimensions(toIndexableAdapters(indexes));
  }

  static List<IndexableAdapter> toIndexableAdapters(List<QueryableIndex> indexes)
  {
    return indexes.stream().map(QueryableIndexIndexableAdapter::new).collect(Collectors.toList());
  }

  static List<String> getMergedDimensions(List<IndexableAdapter> indexes)
  {
    if (indexes.size() == 0) {
      return ImmutableList.of();
    }
    List<String> commonDimOrder = getLongestSharedDimOrder(indexes);
    if (commonDimOrder == null) {
      log.warn("Indexes have incompatible dimension orders, using lexicographic order.");
      return getLexicographicMergedDimensions(indexes);
    } else {
      return commonDimOrder;
    }
  }

  static List<String> getLongestSharedDimOrder(List<IndexableAdapter> indexes)
  {
    int maxSize = 0;
    Iterable<String> orderingCandidate = null;
    for (IndexableAdapter index : indexes) {
      int iterSize = index.getDimensionNames().size();
      if (iterSize > maxSize) {
        maxSize = iterSize;
        orderingCandidate = index.getDimensionNames();
      }
    }

    if (orderingCandidate == null) {
      return null;
    }

    for (IndexableAdapter index : indexes) {
      Iterator<String> candidateIter = orderingCandidate.iterator();
      for (String matchDim : index.getDimensionNames()) {
        boolean matched = false;
        while (candidateIter.hasNext()) {
          String nextDim = candidateIter.next();
          if (matchDim.equals(nextDim)) {
            matched = true;
            break;
          }
        }
        if (!matched) {
          return null;
        }
      }
    }
    return ImmutableList.copyOf(orderingCandidate);
  }

  static List<String> getLexicographicMergedDimensions(List<IndexableAdapter> indexes)
  {
    return mergeIndexed(
        Lists.transform(
            indexes,
            new Function<IndexableAdapter, Iterable<String>>()
            {
              @Override
              public Iterable<String> apply(@Nullable IndexableAdapter input)
              {
                return input.getDimensionNames();
              }
            }
        )
    );
  }

  static <T extends Comparable<? super T>> ArrayList<T> mergeIndexed(List<Iterable<T>> indexedLists)
  {
    Set<T> retVal = Sets.newTreeSet(Comparators.<T>naturalNullsFirst());

    for (Iterable<T> indexedList : indexedLists) {
      for (T val : indexedList) {
        retVal.add(val);
      }
    }

    return Lists.newArrayList(retVal);
  }

  File persist(IncrementalIndex index, File outDir, IndexSpec indexSpec) throws IOException;

  /**
   * This is *not* thread-safe and havok will ensue if this is called and writes are still occurring
   * on the IncrementalIndex object.
   *
   * @param index        the IncrementalIndex to persist
   * @param dataInterval the Interval that the data represents
   * @param outDir       the directory to persist the data to
   *
   * @return the index output directory
   *
   * @throws IOException if an IO error occurs persisting the index
   */
  File persist(IncrementalIndex index, Interval dataInterval, File outDir, IndexSpec indexSpec) throws IOException;

  File persist(
      IncrementalIndex index,
      Interval dataInterval,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException;

  File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException;

  File mergeQueryableIndex(
      List<QueryableIndex> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException;

  File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec
  ) throws IOException;

  File merge(
      List<IndexableAdapter> indexes,
      boolean rollup,
      AggregatorFactory[] metricAggs,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException;

  // Faster than IndexMaker
  File convert(File inDir, File outDir, IndexSpec indexSpec) throws IOException;

  File convert(File inDir, File outDir, IndexSpec indexSpec, ProgressIndicator progress)
      throws IOException;

  File append(List<IndexableAdapter> indexes, AggregatorFactory[] aggregators, File outDir, IndexSpec indexSpec)
      throws IOException;

  File append(
      List<IndexableAdapter> indexes,
      AggregatorFactory[] aggregators,
      File outDir,
      IndexSpec indexSpec,
      ProgressIndicator progress
  ) throws IOException;

  interface IndexSeeker
  {
    int NOT_EXIST = -1;
    int NOT_INIT = -1;

    int seek(int dictId);
  }

  /**
   * Get old dictId from new dictId, and only support access in order
   */
  class IndexSeekerWithConversion implements IndexSeeker
  {
    private final IntBuffer dimConversions;
    private int currIndex;
    private int currVal;
    private int lastVal;

    IndexSeekerWithConversion(IntBuffer dimConversions)
    {
      this.dimConversions = dimConversions;
      this.currIndex = 0;
      this.currVal = IndexSeeker.NOT_INIT;
      this.lastVal = IndexSeeker.NOT_INIT;
    }

    @Override
    public int seek(int dictId)
    {
      if (dimConversions == null) {
        return IndexSeeker.NOT_EXIST;
      }
      if (lastVal != IndexSeeker.NOT_INIT) {
        if (dictId <= lastVal) {
          throw new ISE(
              "Value dictId[%d] is less than the last value dictId[%d] I have, cannot be.",
              dictId, lastVal
          );
        }
        return IndexSeeker.NOT_EXIST;
      }
      if (currVal == IndexSeeker.NOT_INIT) {
        currVal = dimConversions.get();
      }
      if (currVal == dictId) {
        int ret = currIndex;
        ++currIndex;
        if (dimConversions.hasRemaining()) {
          currVal = dimConversions.get();
        } else {
          lastVal = dictId;
        }
        return ret;
      } else if (currVal < dictId) {
        throw new ISE(
            "Skipped currValue dictId[%d], currIndex[%d]; incoming value dictId[%d]",
            currVal, currIndex, dictId
        );
      } else {
        return IndexSeeker.NOT_EXIST;
      }
    }
  }

  class MMappedIndexRowIterable implements Iterable<Rowboat>
  {
    private final Iterable<Rowboat> index;
    private final List<String> convertedDims;
    private final int indexNumber;
    private final List<ColumnCapabilitiesImpl> dimCapabilities;
    private final List<DimensionMerger> mergers;


    MMappedIndexRowIterable(
        Iterable<Rowboat> index,
        List<String> convertedDims,
        int indexNumber,
        final List<ColumnCapabilitiesImpl> dimCapabilities,
        final List<DimensionMerger> mergers
    )
    {
      this.index = index;
      this.convertedDims = convertedDims;
      this.indexNumber = indexNumber;
      this.dimCapabilities = dimCapabilities;
      this.mergers = mergers;
    }

    public Iterable<Rowboat> getIndex()
    {
      return index;
    }

    @Override
    public Iterator<Rowboat> iterator()
    {
      return Iterators.transform(
          index.iterator(),
          new Function<Rowboat, Rowboat>()
          {
            @Override
            public Rowboat apply(@Nullable Rowboat input)
            {
              Object[] dims = input.getDims();
              Object[] newDims = new Object[convertedDims.size()];
              for (int i = 0; i < convertedDims.size(); ++i) {
                if (i >= dims.length) {
                  continue;
                }
                newDims[i] = mergers.get(i).convertSegmentRowValuesToMergedRowValues(dims[i], indexNumber);
              }

              final Rowboat retVal = new Rowboat(
                  input.getTimestamp(),
                  newDims,
                  input.getMetrics(),
                  input.getRowNum(),
                  input.getHandlers()
              );

              retVal.addRow(indexNumber, input.getRowNum());

              return retVal;
            }
          }
      );
    }
  }

  class RowboatMergeFunction implements BinaryFn<Rowboat, Rowboat, Rowboat>
  {
    private final AggregatorFactory[] metricAggs;

    public RowboatMergeFunction(AggregatorFactory[] metricAggs)
    {
      this.metricAggs = metricAggs;
    }

    @Override
    public Rowboat apply(Rowboat lhs, Rowboat rhs)
    {
      if (lhs == null) {
        return rhs;
      }
      if (rhs == null) {
        return lhs;
      }

      Object[] metrics = new Object[metricAggs.length];
      Object[] lhsMetrics = lhs.getMetrics();
      Object[] rhsMetrics = rhs.getMetrics();

      for (int i = 0; i < metrics.length; ++i) {
        Object lhsMetric = lhsMetrics[i];
        Object rhsMetric = rhsMetrics[i];
        if (lhsMetric == null) {
          metrics[i] = rhsMetric;
        } else if (rhsMetric == null) {
          metrics[i] = lhsMetric;
        } else {
          metrics[i] = metricAggs[i].combine(lhsMetric, rhsMetric);
        }
      }

      final Rowboat retVal = new Rowboat(
          lhs.getTimestamp(),
          lhs.getDims(),
          metrics,
          lhs.getRowNum(),
          lhs.getHandlers()
      );

      for (Rowboat rowboat : Arrays.asList(lhs, rhs)) {
        Iterator<Int2ObjectMap.Entry<IntSortedSet>> entryIterator =
            rowboat.getComprisedRows().int2ObjectEntrySet().fastIterator();
        while (entryIterator.hasNext()) {
          Int2ObjectMap.Entry<IntSortedSet> entry = entryIterator.next();

          for (IntIterator setIterator = entry.getValue().iterator(); setIterator.hasNext(); /* NOP */) {
            retVal.addRow(entry.getIntKey(), setIterator.nextInt());
          }
        }
      }

      return retVal;
    }
  }

  class DictionaryMergeIterator implements Iterator<String>
  {
    protected final IntBuffer[] conversions;
    protected final PriorityQueue<Pair<Integer, PeekingIterator<String>>> pQueue;

    protected int counter;

    DictionaryMergeIterator(Indexed<String>[] dimValueLookups, boolean useDirect)
    {
      pQueue = new PriorityQueue<>(
          dimValueLookups.length,
          new Comparator<Pair<Integer, PeekingIterator<String>>>()
          {
            @Override
            public int compare(Pair<Integer, PeekingIterator<String>> lhs, Pair<Integer, PeekingIterator<String>> rhs)
            {
              return lhs.rhs.peek().compareTo(rhs.rhs.peek());
            }
          }
      );
      conversions = new IntBuffer[dimValueLookups.length];
      for (int i = 0; i < conversions.length; i++) {
        if (dimValueLookups[i] == null) {
          continue;
        }
        Indexed<String> indexed = dimValueLookups[i];
        if (useDirect) {
          conversions[i] = ByteBuffer.allocateDirect(indexed.size() * Ints.BYTES).asIntBuffer();
        } else {
          conversions[i] = IntBuffer.allocate(indexed.size());
        }

        final PeekingIterator<String> iter = Iterators.peekingIterator(
            Iterators.transform(
                indexed.iterator(),
                new Function<String, String>()
                {
                  @Override
                  public String apply(@Nullable String input)
                  {
                    return Strings.nullToEmpty(input);
                  }
                }
            )
        );
        if (iter.hasNext()) {
          pQueue.add(Pair.of(i, iter));
        }
      }
    }

    @Override
    public boolean hasNext()
    {
      return !pQueue.isEmpty();
    }

    @Override
    public String next()
    {
      Pair<Integer, PeekingIterator<String>> smallest = pQueue.remove();
      if (smallest == null) {
        throw new NoSuchElementException();
      }
      final String value = writeTranslate(smallest, counter);

      while (!pQueue.isEmpty() && value.equals(pQueue.peek().rhs.peek())) {
        writeTranslate(pQueue.remove(), counter);
      }
      counter++;

      return value;
    }

    boolean needConversion(int index)
    {
      IntBuffer readOnly = conversions[index].asReadOnlyBuffer();
      readOnly.rewind();
      int i = 0;
      while (readOnly.hasRemaining()) {
        if (i != readOnly.get()) {
          return true;
        }
        i++;
      }
      return false;
    }

    private String writeTranslate(Pair<Integer, PeekingIterator<String>> smallest, int counter)
    {
      final int index = smallest.lhs;
      final String value = smallest.rhs.next();

      conversions[index].put(counter);
      if (smallest.rhs.hasNext()) {
        pQueue.add(smallest);
      }
      return value;
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException("remove");
    }
  }
}
