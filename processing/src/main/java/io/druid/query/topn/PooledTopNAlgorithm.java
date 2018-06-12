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

package io.druid.query.topn;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.BaseQuery;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.SimpleDoubleBufferAggregator;
import io.druid.query.monomorphicprocessing.SpecializationService;
import io.druid.query.monomorphicprocessing.SpecializationState;
import io.druid.query.monomorphicprocessing.StringRuntimeShape;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FilteredOffset;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.Offset;
import io.druid.segment.historical.HistoricalColumnSelector;
import io.druid.segment.historical.HistoricalCursor;
import io.druid.segment.historical.HistoricalDimensionSelector;
import io.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 */
public class PooledTopNAlgorithm
    extends BaseTopNAlgorithm<int[], BufferAggregator[], PooledTopNAlgorithm.PooledTopNParams>
{
  private static boolean specializeGeneric1AggPooledTopN =
      !Boolean.getBoolean("dontSpecializeGeneric1AggPooledTopN");
  private static boolean specializeGeneric2AggPooledTopN =
      !Boolean.getBoolean("dontSpecializeGeneric2AggPooledTopN");
  private static boolean specializeHistorical1SimpleDoubleAggPooledTopN =
      !Boolean.getBoolean("dontSpecializeHistorical1SimpleDoubleAggPooledTopN");
  private static boolean specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN =
      !Boolean.getBoolean("dontSpecializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN");

  /**
   * See TopNQueryRunnerTest
   */
  @VisibleForTesting
  static void setSpecializeGeneric1AggPooledTopN(boolean value)
  {
    PooledTopNAlgorithm.specializeGeneric1AggPooledTopN = value;
    computeSpecializedScanAndAggregateImplementations();
  }

  @VisibleForTesting
  static void setSpecializeGeneric2AggPooledTopN(boolean value)
  {
    PooledTopNAlgorithm.specializeGeneric2AggPooledTopN = value;
    computeSpecializedScanAndAggregateImplementations();
  }

  @VisibleForTesting
  static void setSpecializeHistorical1SimpleDoubleAggPooledTopN(boolean value)
  {
    PooledTopNAlgorithm.specializeHistorical1SimpleDoubleAggPooledTopN = value;
    computeSpecializedScanAndAggregateImplementations();
  }

  @VisibleForTesting
  static void setSpecializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN(boolean value)
  {
    PooledTopNAlgorithm.specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN = value;
    computeSpecializedScanAndAggregateImplementations();
  }

  private static final Generic1AggPooledTopNScanner defaultGeneric1AggScanner =
      new Generic1AggPooledTopNScannerPrototype();
  private static final Generic2AggPooledTopNScanner defaultGeneric2AggScanner =
      new Generic2AggPooledTopNScannerPrototype();
  private static final Historical1AggPooledTopNScanner defaultHistorical1SimpleDoubleAggScanner =
      new Historical1SimpleDoubleAggPooledTopNScannerPrototype();
  private static final Historical1AggPooledTopNScanner defaultHistoricalSingleValueDimSelector1SimpleDoubleAggScanner =
      new HistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopNScannerPrototype();

  private interface ScanAndAggregate
  {
    /**
     * If this implementation of ScanAndAggregate is executable with the given parameters, run it and return the number
     * of processed rows. Otherwise return -1 (scanning and aggregation is not done).
     */
    long scanAndAggregate(
        PooledTopNParams params,
        int[] positions,
        BufferAggregator[] theAggregators
    );
  }

  private static final List<ScanAndAggregate> specializedScanAndAggregateImplementations = new ArrayList<>();

  static {
    computeSpecializedScanAndAggregateImplementations();
  }

  private static void computeSpecializedScanAndAggregateImplementations()
  {
    specializedScanAndAggregateImplementations.clear();
    // The order of the following `if` blocks matters, "more specialized" implementations go first
    if (specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN) {
      specializedScanAndAggregateImplementations.add((params, positions, theAggregators) -> {
        if (theAggregators.length == 1) {
          BufferAggregator aggregator = theAggregators[0];
          final Cursor cursor = params.getCursor();
          if (cursor instanceof HistoricalCursor &&
              // FilteredOffset.clone() is not supported. This condition should be removed if
              // HistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopNScannerPrototype
              // doesn't clone offset anymore.
              !(((HistoricalCursor) cursor).getOffset() instanceof FilteredOffset) &&
              aggregator instanceof SimpleDoubleBufferAggregator &&
              params.getDimSelector() instanceof SingleValueHistoricalDimensionSelector &&
              ((SimpleDoubleBufferAggregator) aggregator).getSelector() instanceof HistoricalColumnSelector) {
            return scanAndAggregateHistorical1SimpleDoubleAgg(
                params,
                positions,
                (SimpleDoubleBufferAggregator) aggregator,
                (HistoricalCursor) cursor,
                defaultHistoricalSingleValueDimSelector1SimpleDoubleAggScanner
            );
          }
        }
        return -1;
      });
    }
    if (specializeHistorical1SimpleDoubleAggPooledTopN) {
      specializedScanAndAggregateImplementations.add((params, positions, theAggregators) -> {
        if (theAggregators.length == 1) {
          BufferAggregator aggregator = theAggregators[0];
          final Cursor cursor = params.getCursor();
          if (cursor instanceof HistoricalCursor &&
              // FilteredOffset.clone() is not supported. This condition should be removed if
              // Historical1SimpleDoubleAggPooledTopNScannerPrototype
              // doesn't clone offset anymore.
              !(((HistoricalCursor) cursor).getOffset() instanceof FilteredOffset) &&
              aggregator instanceof SimpleDoubleBufferAggregator &&
              params.getDimSelector() instanceof HistoricalDimensionSelector &&
              ((SimpleDoubleBufferAggregator) aggregator).getSelector() instanceof HistoricalColumnSelector) {
            return scanAndAggregateHistorical1SimpleDoubleAgg(
                params,
                positions,
                (SimpleDoubleBufferAggregator) aggregator,
                (HistoricalCursor) cursor,
                defaultHistorical1SimpleDoubleAggScanner
            );
          }
        }
        return -1;
      });
    }
    if (specializeGeneric1AggPooledTopN) {
      specializedScanAndAggregateImplementations.add((params, positions, theAggregators) -> {
        if (theAggregators.length == 1) {
          return scanAndAggregateGeneric1Agg(params, positions, theAggregators[0], params.getCursor());
        }
        return -1;
      });
    }
    if (specializeGeneric2AggPooledTopN) {
      specializedScanAndAggregateImplementations.add((params, positions, theAggregators) -> {
        if (theAggregators.length == 2) {
          return scanAndAggregateGeneric2Agg(params, positions, theAggregators, params.getCursor());
        }
        return -1;
      });
    }
  }

  private final TopNQuery query;
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private static final int AGG_UNROLL_COUNT = 8; // Must be able to fit loop below

  public PooledTopNAlgorithm(
      StorageAdapter storageAdapter,
      TopNQuery query,
      NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    super(storageAdapter);
    this.query = query;
    this.bufferPool = bufferPool;
  }

  @Override
  public PooledTopNParams makeInitParams(
      ColumnSelectorPlus selectorPlus, Cursor cursor
  )
  {
    ResourceHolder<ByteBuffer> resultsBufHolder = bufferPool.take();
    ByteBuffer resultsBuf = resultsBufHolder.get();
    resultsBuf.clear();

    final DimensionSelector dimSelector = (DimensionSelector) selectorPlus.getSelector();
    final int cardinality = dimSelector.getValueCardinality();

    if (cardinality < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with no dictionary");
    }

    final TopNMetricSpecBuilder<int[]> arrayProvider = new BaseArrayProvider<int[]>(
        dimSelector,
        query,
        storageAdapter
    )
    {
      private final int[] positions = new int[cardinality];

      @Override
      public int[] build()
      {
        Pair<Integer, Integer> startEnd = computeStartEnd(cardinality);

        Arrays.fill(positions, 0, startEnd.lhs, SKIP_POSITION_VALUE);
        Arrays.fill(positions, startEnd.lhs, startEnd.rhs, INIT_POSITION_VALUE);
        Arrays.fill(positions, startEnd.rhs, positions.length, SKIP_POSITION_VALUE);

        return positions;
      }
    };

    final int numBytesToWorkWith = resultsBuf.remaining();
    final int[] aggregatorSizes = new int[query.getAggregatorSpecs().size()];
    int numBytesPerRecord = 0;

    for (int i = 0; i < query.getAggregatorSpecs().size(); ++i) {
      aggregatorSizes[i] = query.getAggregatorSpecs().get(i).getMaxIntermediateSize();
      numBytesPerRecord += aggregatorSizes[i];
    }

    final int numValuesPerPass = numBytesPerRecord > 0 ? numBytesToWorkWith / numBytesPerRecord : cardinality;

    return PooledTopNParams.builder()
                           .withSelectorPlus(selectorPlus)
                           .withCursor(cursor)
                           .withResultsBufHolder(resultsBufHolder)
                           .withResultsBuf(resultsBuf)
                           .withArrayProvider(arrayProvider)
                           .withNumBytesPerRecord(numBytesPerRecord)
                           .withNumValuesPerPass(numValuesPerPass)
                           .withAggregatorSizes(aggregatorSizes)
                           .build();
  }


  @Override
  protected int[] makeDimValSelector(PooledTopNParams params, int numProcessed, int numToProcess)
  {
    final TopNMetricSpecBuilder<int[]> arrayProvider = params.getArrayProvider();

    if (!query.getDimensionSpec().preservesOrdering()) {
      return arrayProvider.build();
    }

    arrayProvider.ignoreFirstN(numProcessed);
    arrayProvider.keepOnlyN(numToProcess);
    return query.getTopNMetricSpec().configureOptimizer(arrayProvider).build();
  }

  @Override
  protected int computeNewLength(int[] dimValSelector, int numProcessed, int numToProcess)
  {
    int valid = 0;
    int length = 0;
    for (int i = numProcessed; i < dimValSelector.length && valid < numToProcess; i++) {
      length++;
      if (SKIP_POSITION_VALUE != dimValSelector[i]) {
        valid++;
      }
    }
    return length;
  }

  @Override
  protected int[] updateDimValSelector(int[] dimValSelector, int numProcessed, int numToProcess)
  {
    final int[] retVal = Arrays.copyOf(dimValSelector, dimValSelector.length);

    final int validEnd = Math.min(retVal.length, numProcessed + numToProcess);
    final int end = Math.max(retVal.length, validEnd);

    Arrays.fill(retVal, 0, numProcessed, SKIP_POSITION_VALUE);
    Arrays.fill(retVal, validEnd, end, SKIP_POSITION_VALUE);

    return retVal;
  }

  @Override
  protected BufferAggregator[] makeDimValAggregateStore(PooledTopNParams params)
  {
    return makeBufferAggregators(params.getCursor(), query.getAggregatorSpecs());
  }

  @Override
  protected long scanAndAggregate(
      final PooledTopNParams params,
      final int[] positions,
      final BufferAggregator[] theAggregators
  )
  {
    for (ScanAndAggregate specializedScanAndAggregate : specializedScanAndAggregateImplementations) {
      long processedRows = specializedScanAndAggregate.scanAndAggregate(params, positions, theAggregators);
      if (processedRows >= 0) {
        BaseQuery.checkInterrupted();
        return processedRows;
      }
    }
    long processedRows = scanAndAggregateDefault(params, positions, theAggregators);
    BaseQuery.checkInterrupted();
    return processedRows;
  }

  private static long scanAndAggregateHistorical1SimpleDoubleAgg(
      PooledTopNParams params,
      int[] positions,
      SimpleDoubleBufferAggregator aggregator,
      HistoricalCursor cursor,
      Historical1AggPooledTopNScanner prototypeScanner
  )
  {
    String runtimeShape = StringRuntimeShape.of(aggregator);
    SpecializationState<Historical1AggPooledTopNScanner> specializationState =
        SpecializationService.getSpecializationState(
            prototypeScanner.getClass(),
            runtimeShape,
            ImmutableMap.of(Offset.class, cursor.getOffset().getClass())
        );
    Historical1AggPooledTopNScanner scanner = specializationState.getSpecializedOrDefault(prototypeScanner);

    long processedRows = scanner.scanAndAggregate(
        (HistoricalDimensionSelector) params.getDimSelector(),
        aggregator.getSelector(),
        aggregator,
        params.getAggregatorSizes()[0],
        cursor,
        positions,
        params.getResultsBuf()
    );
    specializationState.accountLoopIterations(processedRows);
    return processedRows;
  }

  private static long scanAndAggregateGeneric1Agg(
      PooledTopNParams params,
      int[] positions,
      BufferAggregator aggregator,
      Cursor cursor
  )
  {
    String runtimeShape = StringRuntimeShape.of(aggregator);
    Class<? extends Generic1AggPooledTopNScanner> prototypeClass = Generic1AggPooledTopNScannerPrototype.class;
    SpecializationState<Generic1AggPooledTopNScanner> specializationState = SpecializationService
        .getSpecializationState(prototypeClass, runtimeShape);
    Generic1AggPooledTopNScanner scanner = specializationState.getSpecializedOrDefault(defaultGeneric1AggScanner);
    long processedRows = scanner.scanAndAggregate(
        params.getDimSelector(),
        aggregator,
        params.getAggregatorSizes()[0],
        cursor,
        positions,
        params.getResultsBuf()
    );
    specializationState.accountLoopIterations(processedRows);
    return processedRows;
  }

  private static long scanAndAggregateGeneric2Agg(
      PooledTopNParams params,
      int[] positions,
      BufferAggregator[] theAggregators,
      Cursor cursor
  )
  {
    String runtimeShape = StringRuntimeShape.of(theAggregators);
    Class<? extends Generic2AggPooledTopNScanner> prototypeClass = Generic2AggPooledTopNScannerPrototype.class;
    SpecializationState<Generic2AggPooledTopNScanner> specializationState = SpecializationService
        .getSpecializationState(prototypeClass, runtimeShape);
    Generic2AggPooledTopNScanner scanner = specializationState.getSpecializedOrDefault(defaultGeneric2AggScanner);
    int[] aggregatorSizes = params.getAggregatorSizes();
    long processedRows = scanner.scanAndAggregate(
        params.getDimSelector(),
        theAggregators[0],
        aggregatorSizes[0],
        theAggregators[1],
        aggregatorSizes[1],
        cursor,
        positions,
        params.getResultsBuf()
    );
    specializationState.accountLoopIterations(processedRows);
    return processedRows;
  }

  /**
   * Use aggressive loop unrolling to aggregate the data
   *
   * How this works: The aggregates are evaluated AGG_UNROLL_COUNT at a time. This was chosen to be 8 rather arbitrarily.
   * The offsets into the output buffer are precalculated and stored in aggregatorOffsets
   *
   * For queries whose aggregate count is less than AGG_UNROLL_COUNT, the aggregates evaluted in a switch statement.
   * See http://en.wikipedia.org/wiki/Duff's_device for more information on this kind of approach
   *
   * This allows out of order execution of the code. In local tests, the JVM inlines all the way to this function.
   *
   * If there are more than AGG_UNROLL_COUNT aggregates, then the remainder is calculated with the switch, and the
   * blocks of AGG_UNROLL_COUNT are calculated in a partially unrolled for-loop.
   *
   * Putting the switch first allows for optimization for the common case (less than AGG_UNROLL_COUNT aggs) but
   * still optimizes the high quantity of aggregate queries which benefit greatly from any speed improvements
   * (they simply take longer to start with).
   */
  private static long scanAndAggregateDefault(
      final PooledTopNParams params,
      final int[] positions,
      final BufferAggregator[] theAggregators
  )
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }

    final ByteBuffer resultsBuf = params.getResultsBuf();
    final int numBytesPerRecord = params.getNumBytesPerRecord();
    final int[] aggregatorSizes = params.getAggregatorSizes();
    final Cursor cursor = params.getCursor();
    final DimensionSelector dimSelector = params.getDimSelector();

    final int[] aggregatorOffsets = new int[aggregatorSizes.length];
    for (int j = 0, offset = 0; j < aggregatorSizes.length; ++j) {
      aggregatorOffsets[j] = offset;
      offset += aggregatorSizes[j];
    }

    final int aggSize = theAggregators.length;
    final int aggExtra = aggSize % AGG_UNROLL_COUNT;
    int currentPosition = 0;
    long processedRows = 0;
    while (!cursor.isDoneOrInterrupted()) {
      final IndexedInts dimValues = dimSelector.getRow();

      final int dimSize = dimValues.size();
      final int dimExtra = dimSize % AGG_UNROLL_COUNT;
      switch (dimExtra) {
        case 7:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(6),
              currentPosition
          );
          // fall through
        case 6:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(5),
              currentPosition
          );
          // fall through
        case 5:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(4),
              currentPosition
          );
          // fall through
        case 4:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(3),
              currentPosition
          );
          // fall through
        case 3:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(2),
              currentPosition
          );
          // fall through
        case 2:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(1),
              currentPosition
          );
          // fall through
        case 1:
          currentPosition = aggregateDimValue(
              positions,
              theAggregators,
              resultsBuf,
              numBytesPerRecord,
              aggregatorOffsets,
              aggSize,
              aggExtra,
              dimValues.get(0),
              currentPosition
          );
      }
      for (int i = dimExtra; i < dimSize; i += AGG_UNROLL_COUNT) {
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 1),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 2),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 3),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 4),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 5),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 6),
            currentPosition
        );
        currentPosition = aggregateDimValue(
            positions,
            theAggregators,
            resultsBuf,
            numBytesPerRecord,
            aggregatorOffsets,
            aggSize,
            aggExtra,
            dimValues.get(i + 7),
            currentPosition
        );
      }
      cursor.advanceUninterruptibly();
      processedRows++;
    }
    return processedRows;
  }

  /**
   * Returns a new currentPosition, incremented if a new position was initialized, otherwise the same position as passed
   * in the last argument.
   */
  private static int aggregateDimValue(
      final int[] positions,
      final BufferAggregator[] theAggregators,
      final ByteBuffer resultsBuf,
      final int numBytesPerRecord,
      final int[] aggregatorOffsets,
      final int aggSize,
      final int aggExtra,
      final int dimIndex,
      int currentPosition
  )
  {
    if (SKIP_POSITION_VALUE == positions[dimIndex]) {
      return currentPosition;
    }
    if (INIT_POSITION_VALUE == positions[dimIndex]) {
      positions[dimIndex] = currentPosition * numBytesPerRecord;
      currentPosition++;
      final int pos = positions[dimIndex];
      for (int j = 0; j < aggSize; ++j) {
        theAggregators[j].init(resultsBuf, pos + aggregatorOffsets[j]);
      }
    }
    final int position = positions[dimIndex];

    switch (aggExtra) {
      case 7:
        theAggregators[6].aggregate(resultsBuf, position + aggregatorOffsets[6]);
        // fall through
      case 6:
        theAggregators[5].aggregate(resultsBuf, position + aggregatorOffsets[5]);
        // fall through
      case 5:
        theAggregators[4].aggregate(resultsBuf, position + aggregatorOffsets[4]);
        // fall through
      case 4:
        theAggregators[3].aggregate(resultsBuf, position + aggregatorOffsets[3]);
        // fall through
      case 3:
        theAggregators[2].aggregate(resultsBuf, position + aggregatorOffsets[2]);
        // fall through
      case 2:
        theAggregators[1].aggregate(resultsBuf, position + aggregatorOffsets[1]);
        // fall through
      case 1:
        theAggregators[0].aggregate(resultsBuf, position + aggregatorOffsets[0]);
    }
    for (int j = aggExtra; j < aggSize; j += AGG_UNROLL_COUNT) {
      theAggregators[j].aggregate(resultsBuf, position + aggregatorOffsets[j]);
      theAggregators[j + 1].aggregate(resultsBuf, position + aggregatorOffsets[j + 1]);
      theAggregators[j + 2].aggregate(resultsBuf, position + aggregatorOffsets[j + 2]);
      theAggregators[j + 3].aggregate(resultsBuf, position + aggregatorOffsets[j + 3]);
      theAggregators[j + 4].aggregate(resultsBuf, position + aggregatorOffsets[j + 4]);
      theAggregators[j + 5].aggregate(resultsBuf, position + aggregatorOffsets[j + 5]);
      theAggregators[j + 6].aggregate(resultsBuf, position + aggregatorOffsets[j + 6]);
      theAggregators[j + 7].aggregate(resultsBuf, position + aggregatorOffsets[j + 7]);
    }
    return currentPosition;
  }

  @Override
  protected void updateResults(
      PooledTopNParams params,
      int[] positions,
      BufferAggregator[] theAggregators,
      TopNResultBuilder resultBuilder
  )
  {
    if (params.getCardinality() < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with unknown cardinality");
    }

    final ByteBuffer resultsBuf = params.getResultsBuf();
    final int[] aggregatorSizes = params.getAggregatorSizes();
    final DimensionSelector dimSelector = params.getDimSelector();

    final ValueType outType = query.getDimensionSpec().getOutputType();
    final boolean needsResultConversion = outType != ValueType.STRING;
    final Function<Object, Object> valueTransformer = TopNMapFn.getValueTransformer(outType);

    for (int i = 0; i < positions.length; i++) {
      int position = positions[i];
      if (position >= 0) {
        Object[] vals = new Object[theAggregators.length];
        for (int j = 0; j < theAggregators.length; j++) {
          vals[j] = theAggregators[j].get(resultsBuf, position);
          position += aggregatorSizes[j];
        }

        Object retVal = dimSelector.lookupName(i);
        if (needsResultConversion) {
          retVal = valueTransformer.apply(retVal);
        }


        resultBuilder.addEntry(
            (Comparable) retVal,
            i,
            vals
        );
      }
    }
  }

  @Override
  protected void closeAggregators(BufferAggregator[] bufferAggregators)
  {
    for (BufferAggregator agg : bufferAggregators) {
      agg.close();
    }
  }

  @Override
  public void cleanup(PooledTopNParams params)
  {
    if (params != null) {
      ResourceHolder<ByteBuffer> resultsBufHolder = params.getResultsBufHolder();

      if (resultsBufHolder != null) {
        resultsBufHolder.get().clear();
      }
      CloseQuietly.close(resultsBufHolder);
    }
  }

  public static class PooledTopNParams extends TopNParams
  {
    private final ResourceHolder<ByteBuffer> resultsBufHolder;
    private final ByteBuffer resultsBuf;
    private final int[] aggregatorSizes;
    private final int numBytesPerRecord;
    private final TopNMetricSpecBuilder<int[]> arrayProvider;

    public PooledTopNParams(
        ColumnSelectorPlus selectorPlus,
        Cursor cursor,
        ResourceHolder<ByteBuffer> resultsBufHolder,
        ByteBuffer resultsBuf,
        int[] aggregatorSizes,
        int numBytesPerRecord,
        int numValuesPerPass,
        TopNMetricSpecBuilder<int[]> arrayProvider
    )
    {
      super(selectorPlus, cursor, numValuesPerPass);

      this.resultsBufHolder = resultsBufHolder;
      this.resultsBuf = resultsBuf;
      this.aggregatorSizes = aggregatorSizes;
      this.numBytesPerRecord = numBytesPerRecord;
      this.arrayProvider = arrayProvider;
    }

    public static Builder builder()
    {
      return new Builder();
    }

    public ResourceHolder<ByteBuffer> getResultsBufHolder()
    {
      return resultsBufHolder;
    }

    public ByteBuffer getResultsBuf()
    {
      return resultsBuf;
    }

    public int[] getAggregatorSizes()
    {
      return aggregatorSizes;
    }

    public int getNumBytesPerRecord()
    {
      return numBytesPerRecord;
    }

    public TopNMetricSpecBuilder<int[]> getArrayProvider()
    {
      return arrayProvider;
    }

    public static class Builder
    {
      private ColumnSelectorPlus selectorPlus;
      private Cursor cursor;
      private ResourceHolder<ByteBuffer> resultsBufHolder;
      private ByteBuffer resultsBuf;
      private int[] aggregatorSizes;
      private int numBytesPerRecord;
      private int numValuesPerPass;
      private TopNMetricSpecBuilder<int[]> arrayProvider;

      public Builder()
      {
        selectorPlus = null;
        cursor = null;
        resultsBufHolder = null;
        resultsBuf = null;
        aggregatorSizes = null;
        numBytesPerRecord = 0;
        numValuesPerPass = 0;
        arrayProvider = null;
      }

      public Builder withSelectorPlus(ColumnSelectorPlus selectorPlus)
      {
        this.selectorPlus = selectorPlus;
        return this;
      }

      public Builder withCursor(Cursor cursor)
      {
        this.cursor = cursor;
        return this;
      }

      public Builder withResultsBufHolder(ResourceHolder<ByteBuffer> resultsBufHolder)
      {
        this.resultsBufHolder = resultsBufHolder;
        return this;
      }

      public Builder withResultsBuf(ByteBuffer resultsBuf)
      {
        this.resultsBuf = resultsBuf;
        return this;
      }

      public Builder withAggregatorSizes(int[] aggregatorSizes)
      {
        this.aggregatorSizes = aggregatorSizes;
        return this;
      }

      public Builder withNumBytesPerRecord(int numBytesPerRecord)
      {
        this.numBytesPerRecord = numBytesPerRecord;
        return this;
      }

      public Builder withNumValuesPerPass(int numValuesPerPass)
      {
        this.numValuesPerPass = numValuesPerPass;
        return this;
      }

      public Builder withArrayProvider(TopNMetricSpecBuilder<int[]> arrayProvider)
      {
        this.arrayProvider = arrayProvider;
        return this;
      }

      public PooledTopNParams build()
      {
        return new PooledTopNParams(
            selectorPlus,
            cursor,
            resultsBufHolder,
            resultsBuf,
            aggregatorSizes,
            numBytesPerRecord,
            numValuesPerPass,
            arrayProvider
        );
      }
    }
  }
}
