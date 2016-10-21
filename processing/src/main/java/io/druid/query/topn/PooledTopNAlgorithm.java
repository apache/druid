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

import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class PooledTopNAlgorithm
    extends BaseTopNAlgorithm<int[], BufferAggregator[], PooledTopNAlgorithm.PooledTopNParams>
{
  private final Capabilities capabilities;
  private final TopNQuery query;
  private final StupidPool<ByteBuffer> bufferPool;
  private static final int AGG_UNROLL_COUNT = 8; // Must be able to fit loop below

  public PooledTopNAlgorithm(
      Capabilities capabilities,
      TopNQuery query,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(capabilities);

    this.capabilities = capabilities;
    this.query = query;
    this.bufferPool = bufferPool;
  }

  @Override
  public PooledTopNParams makeInitParams(
      DimensionSelector dimSelector, Cursor cursor
  )
  {
    ResourceHolder<ByteBuffer> resultsBufHolder = bufferPool.take();
    ByteBuffer resultsBuf = resultsBufHolder.get();
    resultsBuf.clear();

    final int cardinality = dimSelector.getValueCardinality();

    if (cardinality < 0) {
      throw new UnsupportedOperationException("Cannot operate on a dimension with no dictionary");
    }

    final TopNMetricSpecBuilder<int[]> arrayProvider = new BaseArrayProvider<int[]>(
        dimSelector,
        query,
        capabilities
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
                           .withDimSelector(dimSelector)
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
  @Override
  protected void scanAndAggregate(
      final PooledTopNParams params,
      final int[] positions,
      final BufferAggregator[] theAggregators,
      final int numProcessed
  )
  {
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

    while (!cursor.isDone()) {
      final IndexedInts dimValues = dimSelector.getRow();

      final int dimSize = dimValues.size();
      final int dimExtra = dimSize % AGG_UNROLL_COUNT;
      switch(dimExtra){
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
      cursor.advance();
    }
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

    switch(aggExtra) {
      case 7:
        theAggregators[6].aggregate(resultsBuf, position + aggregatorOffsets[6]);
      case 6:
        theAggregators[5].aggregate(resultsBuf, position + aggregatorOffsets[5]);
      case 5:
        theAggregators[4].aggregate(resultsBuf, position + aggregatorOffsets[4]);
      case 4:
        theAggregators[3].aggregate(resultsBuf, position + aggregatorOffsets[3]);
      case 3:
        theAggregators[2].aggregate(resultsBuf, position + aggregatorOffsets[2]);
      case 2:
        theAggregators[1].aggregate(resultsBuf, position + aggregatorOffsets[1]);
      case 1:
        theAggregators[0].aggregate(resultsBuf, position + aggregatorOffsets[0]);
    }
    for (int j = aggExtra; j < aggSize; j += AGG_UNROLL_COUNT) {
      theAggregators[j].aggregate(resultsBuf, position + aggregatorOffsets[j]);
      theAggregators[j+1].aggregate(resultsBuf, position + aggregatorOffsets[j+1]);
      theAggregators[j+2].aggregate(resultsBuf, position + aggregatorOffsets[j+2]);
      theAggregators[j+3].aggregate(resultsBuf, position + aggregatorOffsets[j+3]);
      theAggregators[j+4].aggregate(resultsBuf, position + aggregatorOffsets[j+4]);
      theAggregators[j+5].aggregate(resultsBuf, position + aggregatorOffsets[j+5]);
      theAggregators[j+6].aggregate(resultsBuf, position + aggregatorOffsets[j+6]);
      theAggregators[j+7].aggregate(resultsBuf, position + aggregatorOffsets[j+7]);
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
    final ByteBuffer resultsBuf = params.getResultsBuf();
    final int[] aggregatorSizes = params.getAggregatorSizes();
    final DimensionSelector dimSelector = params.getDimSelector();

    for (int i = 0; i < positions.length; i++) {
      int position = positions[i];
      if (position >= 0) {
        Object[] vals = new Object[theAggregators.length];
        for (int j = 0; j < theAggregators.length; j++) {
          vals[j] = theAggregators[j].get(resultsBuf, position);
          position += aggregatorSizes[j];
        }

        resultBuilder.addEntry(
            dimSelector.lookupName(i),
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
        DimensionSelector dimSelector,
        Cursor cursor,
        ResourceHolder<ByteBuffer> resultsBufHolder,
        ByteBuffer resultsBuf,
        int[] aggregatorSizes,
        int numBytesPerRecord,
        int numValuesPerPass,
        TopNMetricSpecBuilder<int[]> arrayProvider
    )
    {
      super(dimSelector, cursor, numValuesPerPass);

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
      private DimensionSelector dimSelector;
      private Cursor cursor;
      private ResourceHolder<ByteBuffer> resultsBufHolder;
      private ByteBuffer resultsBuf;
      private int[] aggregatorSizes;
      private int numBytesPerRecord;
      private int numValuesPerPass;
      private TopNMetricSpecBuilder<int[]> arrayProvider;

      public Builder()
      {
        dimSelector = null;
        cursor = null;
        resultsBufHolder = null;
        resultsBuf = null;
        aggregatorSizes = null;
        numBytesPerRecord = 0;
        numValuesPerPass = 0;
        arrayProvider = null;
      }

      public Builder withDimSelector(DimensionSelector dimSelector)
      {
        this.dimSelector = dimSelector;
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
            dimSelector,
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
