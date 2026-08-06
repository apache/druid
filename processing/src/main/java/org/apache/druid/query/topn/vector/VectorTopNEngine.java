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

package org.apache.druid.query.topn.vector;

import com.google.common.base.Suppliers;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.Order;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.epinephelinae.BufferArrayGrouper;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.epinephelinae.HeapVectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.VectorGrouper;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.query.vector.VectorCursorGranularizer;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.TimeBoundaryInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorCursor;
import org.apache.druid.segment.virtual.VirtualizedColumnInspector;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Vectorized execution engine for {@link TopNQuery}, analogous to
 * {@link org.apache.druid.query.groupby.epinephelinae.vector.VectorGroupByEngine} for groupBy.
 *
 * Uses a {@link VectorGrouper} for batch aggregation (with vectorized null handling via
 * {@link org.apache.druid.segment.vector.VectorValueSelector#getNullVector()}) and then applies top-N
 * ordering via {@link TopNResultBuilder} after each time-bucket is fully aggregated.
 *
 * @see org.apache.druid.query.topn.TopNQueryEngine for the entry point that selects this path
 */
public class VectorTopNEngine
{
  private VectorTopNEngine()
  {
    // No instantiation.
  }

  public static Sequence<Result<TopNResultValue>> process(
      final TopNQuery query,
      @Nullable final TimeBoundaryInspector timeBoundaryInspector,
      final CursorHolder cursorHolder,
      final ByteBuffer processingBuffer
  )
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Result<TopNResultValue>, CloseableIterator<Result<TopNResultValue>>>()
        {
          @Override
          public CloseableIterator<Result<TopNResultValue>> make()
          {
            final VectorCursor cursor = cursorHolder.asVectorCursor();

            if (cursor == null) {
              return new CloseableIterator<>()
              {
                @Override
                public boolean hasNext()
                {
                  return false;
                }

                @Override
                public Result<TopNResultValue> next()
                {
                  throw new NoSuchElementException();
                }

                @Override
                public void close()
                {
                  // Nothing to do.
                }
              };
            }

            final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
            final TopNVectorColumnSelector selector = ColumnProcessors.makeVectorProcessor(
                query.getDimensionSpec(),
                TopNVectorColumnProcessorFactory.instance(),
                columnSelectorFactory
            );

            return new VectorTopNEngineIterator(
                query,
                timeBoundaryInspector,
                cursor,
                cursorHolder.getTimeOrder(),
                selector,
                processingBuffer
            );
          }

          @Override
          public void cleanup(final CloseableIterator<Result<TopNResultValue>> iterFromMake)
          {
            try {
              iterFromMake.close();
            }
            catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }
    );
  }

  /**
   * Returns true if the given query is eligible for the vectorized topN path.
   */
  public static boolean canVectorize(final TopNQuery query, final ColumnInspector inspector)
  {
    final DimensionSpec dimensionSpec = query.getDimensionSpec();

    if (!dimensionSpec.canVectorize()) {
      return false;
    }

    // Decorated specs (e.g. extraction functions that are not one-to-one) change value semantics in ways that
    // are incompatible with the vectorized grouper key approach.
    if (dimensionSpec.mustDecorate()) {
      return false;
    }

    if (dimensionSpec.getOutputType().isArray()) {
      return false;
    }

    // Wrap with virtual columns so capabilities lookups for virtual column dimensions work correctly.
    final ColumnInspector effectiveInspector =
        new VirtualizedColumnInspector(inspector, query.getVirtualColumns());

    final ColumnCapabilities capabilities = effectiveInspector.getColumnCapabilities(dimensionSpec.getDimension());
    // null means column does not exist; nil columns can be vectorized
    if (capabilities != null && capabilities.hasMultipleValues().isMaybeTrue()) {
      return false;
    }

    // COMPLEX columns route to makeObjectProcessor which only handles STRING; reject them here so the
    // capability check matches what the factory actually supports.
    if (capabilities != null && capabilities.is(ValueType.COMPLEX)) {
      return false;
    }

    // TODO(vectorized-topn): the non-vectorized path coerces raw values to the dimension's output type before
    // grouping (see TopNColumnAggregatesProcessorFactory). This path groups on the raw column type, so mixed-type
    // queries (e.g. DOUBLE column with LONG output) would produce distinct groups that coerce to the same output
    // value. Falling back for now; a future change could coerce at writeKeys time to match non-vec semantics.
    if (capabilities != null && dimensionSpec.getOutputType().getType() != capabilities.getType()) {
      return false;
    }

    for (final AggregatorFactory agg : query.getAggregatorSpecs()) {
      if (!agg.canVectorize(effectiveInspector)) {
        return false;
      }
    }

    return true;
  }

  static class VectorTopNEngineIterator implements CloseableIterator<Result<TopNResultValue>>
  {
    private final TopNQuery query;
    private final VectorCursor cursor;
    private final TopNVectorColumnSelector selector;
    private final ByteBuffer processingBuffer;
    private final int keySize;
    private final WritableMemory keySpace;
    private final Comparator comparator;
    private final VectorGrouper grouper;

    @Nullable
    private final VectorCursorGranularizer granularizer;

    private final Iterator<Interval> bucketIterator;

    @Nullable
    private Interval bucketInterval;

    VectorTopNEngineIterator(
        final TopNQuery query,
        @Nullable final TimeBoundaryInspector timeBoundaryInspector,
        final VectorCursor cursor,
        final Order timeOrder,
        final TopNVectorColumnSelector selector,
        final ByteBuffer processingBuffer
    )
    {
      this.query = query;
      this.cursor = cursor;
      this.selector = selector;
      this.processingBuffer = processingBuffer;
      this.keySize = selector.getGroupingKeySize();
      this.keySpace = WritableMemory.allocate(keySize * cursor.getMaxVectorSize());
      this.comparator = query.getTopNMetricSpec().getComparator(
          query.getAggregatorSpecs(),
          query.getPostAggregatorSpecs()
      );
      this.granularizer = VectorCursorGranularizer.create(
          cursor,
          timeBoundaryInspector,
          timeOrder,
          query.getGranularity(),
          query.getSingleInterval()
      );

      if (granularizer != null) {
        this.bucketIterator = granularizer.getBucketIterable().iterator();
      } else {
        this.bucketIterator = Collections.emptyIterator();
      }

      this.bucketInterval = bucketIterator.hasNext() ? bucketIterator.next() : null;
      this.grouper = makeGrouper();
    }

    @Override
    public boolean hasNext()
    {
      // Return true for any remaining bucket, even if cursor is done — we still emit empty results.
      return bucketInterval != null;
    }

    @Override
    public Result<TopNResultValue> next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      // setCurrentOffsets must be called at the start of each vector batch to keep offsets current.
      while (!cursor.isDone()) {
        granularizer.setCurrentOffsets(bucketInterval);
        final int startOffset = granularizer.getStartOffset();
        final int endOffset = granularizer.getEndOffset();

        if (endOffset > startOffset) {
          selector.writeKeys(keySpace, keySize, 0, startOffset, endOffset);
          grouper.aggregateVector(keySpace, startOffset, endOffset);
        }

        if (!granularizer.advanceCursorWithinBucket()) {
          break;
        }
      }

      final DateTime bucketTimestamp = query.getGranularity().toDateTime(bucketInterval.getStartMillis());
      final TopNResultBuilder resultBuilder = query.getTopNMetricSpec().getResultBuilder(
          bucketTimestamp,
          query.getDimensionSpec(),
          query.getThreshold(),
          comparator,
          query.getAggregatorSpecs(),
          query.getPostAggregatorSpecs()
      );

      try (final CloseableIterator<Grouper.Entry<MemoryPointer>> iter = grouper.iterator()) {
        while (iter.hasNext()) {
          final Grouper.Entry<MemoryPointer> entry = iter.next();
          final Object dimValue = selector.getDimensionValue(entry.getKey(), 0);
          resultBuilder.addEntry(dimValue, dimValue, entry.getValues());
        }
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      bucketInterval = bucketIterator.hasNext() ? bucketIterator.next() : null;
      selector.reset();
      grouper.reset();
      return resultBuilder.build();
    }

    @Override
    public void close() throws IOException
    {
      // Cursor is closed by the CursorHolder, which is closed by the calling sequence's baggage.
      grouper.close();
    }

    private VectorGrouper makeGrouper()
    {
      final VectorColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
      final List<AggregatorFactory> aggFactories = query.getAggregatorSpecs();
      final AggregatorAdapters aggregators = AggregatorAdapters.factorizeVector(columnSelectorFactory, aggFactories);

      // Use BufferArrayGrouper when the dimension is a dictionary-encoded string with small enough known cardinality
      // to fit in the processing buffer — avoids hash-table overhead for the most common topN case.
      final int cardinalityForArray = getCardinalityForArrayAggregation(aggFactories);
      final VectorGrouper grouper;
      if (cardinalityForArray >= 0) {
        grouper = new BufferArrayGrouper(Suppliers.ofInstance(processingBuffer), aggregators, cardinalityForArray);
      } else {
        // Heap-backed grouper grows on demand; topN cannot accept partial aggregation, unlike groupBy.
        grouper = new HeapVectorGrouper(aggregators, keySize);
      }

      grouper.initVectorized(cursor.getMaxVectorSize());
      return grouper;
    }

    private int getCardinalityForArrayAggregation(final List<AggregatorFactory> aggFactories)
    {
      final ColumnCapabilities capabilities = cursor.getColumnSelectorFactory()
                                                    .getColumnCapabilities(query.getDimensionSpec().getDimension());
      if (!Types.is(capabilities, ValueType.STRING)) {
        return -1;
      }

      // Virtual columns shadow real columns; a virtual column cannot report its own cardinality.
      if (query.getVirtualColumns().exists(query.getDimensionSpec().getDimension())) {
        return -1;
      }

      final int cardinality = selector.getValueCardinality();
      if (cardinality <= 0) {
        return -1;
      }

      final long requiredBufferCapacity = BufferArrayGrouper.requiredBufferCapacity(
          cardinality,
          aggFactories.toArray(new AggregatorFactory[0])
      );
      if (requiredBufferCapacity < 0 || requiredBufferCapacity > processingBuffer.capacity()) {
        return -1;
      }

      return cardinality;
    }
  }
}
