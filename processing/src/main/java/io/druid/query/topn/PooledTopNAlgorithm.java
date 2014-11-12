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

package io.druid.query.topn;

import com.metamx.common.Pair;
import com.metamx.common.guava.CloseQuietly;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
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

    final int numValuesPerPass = numBytesToWorkWith / numBytesPerRecord;

    return PooledTopNParams.builder()
                           .withDimSelector(dimSelector)
                           .withCursor(cursor)
                           .withCardinality(cardinality)
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

    while (!cursor.isDone()) {
      final IndexedInts dimValues = dimSelector.getRow();

      final int dimSize = dimValues.size();
      final int dimExtra = dimSize % AGG_UNROLL_COUNT;
      switch(dimExtra){
        case 7:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(6));
        case 6:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(5));
        case 5:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(4));
        case 4:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(3));
        case 3:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(2));
        case 2:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(1));
        case 1:
          aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(0));
      }
      for (int i = dimExtra; i < dimSize; i += AGG_UNROLL_COUNT) {
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+1));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+2));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+3));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+4));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+5));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+6));
        aggregateDimValue(positions, theAggregators, numProcessed, resultsBuf, numBytesPerRecord, aggregatorOffsets, aggSize, aggExtra, dimValues.get(i+7));
      }
      cursor.advance();
    }
  }

  private static void aggregateDimValue(
      final int[] positions,
      final BufferAggregator[] theAggregators,
      final int numProcessed,
      final ByteBuffer resultsBuf,
      final int numBytesPerRecord,
      final int[] aggregatorOffsets,
      final int aggSize,
      final int aggExtra,
      final int dimIndex
  )
  {
    if (SKIP_POSITION_VALUE == positions[dimIndex]) {
      return;
    }
    if (INIT_POSITION_VALUE == positions[dimIndex]) {
      positions[dimIndex] = (dimIndex - numProcessed) * numBytesPerRecord;
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
    ResourceHolder<ByteBuffer> resultsBufHolder = params.getResultsBufHolder();

    if (resultsBufHolder != null) {
      resultsBufHolder.get().clear();
    }
    CloseQuietly.close(resultsBufHolder);
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
        int cardinality,
        ResourceHolder<ByteBuffer> resultsBufHolder,
        ByteBuffer resultsBuf,
        int[] aggregatorSizes,
        int numBytesPerRecord,
        int numValuesPerPass,
        TopNMetricSpecBuilder<int[]> arrayProvider
    )
    {
      super(dimSelector, cursor, cardinality, numValuesPerPass);

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
      private int cardinality;
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
        cardinality = 0;
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

      public Builder withCardinality(int cardinality)
      {
        this.cardinality = cardinality;
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
            cardinality,
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
