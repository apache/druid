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

package org.apache.druid.query.aggregation;

import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class that helps query engines use Buffer- or VectorAggregators in a consistent way.
 *
 * The two main benefits this class provides are:
 *
 * (1) Query engines can treat BufferAggregators and VectorAggregators the same for operations that are equivalent
 * across them, like "init", "get", "relocate", and "close".
 * (2) Query engines are freed from the need to manage how much space each individual aggregator needs. They only
 * need to allocate a block of size "spaceNeeded".
 */
public class AggregatorAdapters implements Closeable
{
  private static final Logger log = new Logger(AggregatorAdapters.class);

  private final List<AggregatorAdapter> adapters;
  private final List<AggregatorFactory> factories;
  private final int[] aggregatorPositions;
  private final int spaceNeeded;

  private AggregatorAdapters(final List<AggregatorAdapter> adapters)
  {
    this.adapters = adapters;
    this.factories = adapters.stream().map(AggregatorAdapter::getFactory).collect(Collectors.toList());
    this.aggregatorPositions = new int[adapters.size()];

    long nextPosition = 0;
    for (int i = 0; i < adapters.size(); i++) {
      final AggregatorFactory aggregatorFactory = adapters.get(i).getFactory();
      aggregatorPositions[i] = Ints.checkedCast(nextPosition);
      nextPosition += aggregatorFactory.getMaxIntermediateSizeWithNulls();
    }

    this.spaceNeeded = Ints.checkedCast(nextPosition);
  }

  public static AggregatorAdapters factorizeVector(
      final VectorColumnSelectorFactory columnSelectorFactory,
      final List<AggregatorFactory> aggregatorFactories
  )
  {
    final AggregatorAdapter[] adapters = new AggregatorAdapter[aggregatorFactories.size()];
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorFactories.get(i);
      adapters[i] = new VectorAggregatorAdapter(
          aggregatorFactory,
          aggregatorFactory.factorizeVector(columnSelectorFactory)
      );
    }

    return new AggregatorAdapters(Arrays.asList(adapters));
  }

  public static AggregatorAdapters factorizeBuffered(
      final ColumnSelectorFactory columnSelectorFactory,
      final List<AggregatorFactory> aggregatorFactories
  )
  {
    final AggregatorAdapter[] adapters = new AggregatorAdapter[aggregatorFactories.size()];
    for (int i = 0; i < aggregatorFactories.size(); i++) {
      final AggregatorFactory aggregatorFactory = aggregatorFactories.get(i);
      adapters[i] = new BufferAggregatorAdapter(
          aggregatorFactory,
          aggregatorFactory.factorizeBuffered(columnSelectorFactory)
      );
    }

    return new AggregatorAdapters(Arrays.asList(adapters));
  }

  public int spaceNeeded()
  {
    return spaceNeeded;
  }

  public List<AggregatorFactory> factories()
  {
    return factories;
  }

  public int[] aggregatorPositions()
  {
    return aggregatorPositions;
  }

  public int size()
  {
    return adapters.size();
  }

  public void init(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < adapters.size(); i++) {
      adapters.get(i).init(buf, position + aggregatorPositions[i]);
    }
  }

  public void aggregateBuffered(final ByteBuffer buf, final int position)
  {
    for (int i = 0; i < adapters.size(); i++) {
      final AggregatorAdapter adapter = adapters.get(i);
      adapter.asBufferAggregator().aggregate(buf, position + aggregatorPositions[i]);
    }
  }

  public void aggregateVector(
      final ByteBuffer buf,
      final int position,
      final int start,
      final int end
  )
  {
    for (int i = 0; i < adapters.size(); i++) {
      final AggregatorAdapter adapter = adapters.get(i);
      adapter.asVectorAggregator().aggregate(buf, position + aggregatorPositions[i], start, end);
    }
  }

  public void aggregateVector(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows
  )
  {
    for (int i = 0; i < adapters.size(); i++) {
      final AggregatorAdapter adapter = adapters.get(i);
      adapter.asVectorAggregator().aggregate(buf, numRows, positions, rows, aggregatorPositions[i]);
    }
  }

  @Nullable
  public Object get(final ByteBuffer buf, final int position, final int aggregatorNumber)
  {
    return adapters.get(aggregatorNumber).get(buf, position + aggregatorPositions[aggregatorNumber]);
  }

  public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
  {
    for (int i = 0; i < adapters.size(); i++) {
      adapters.get(i).relocate(
          oldPosition + aggregatorPositions[i],
          newPosition + aggregatorPositions[i],
          oldBuffer,
          newBuffer
      );
    }
  }

  @Override
  public void close()
  {
    for (AggregatorAdapter adapter : adapters) {
      try {
        adapter.close();
      }
      catch (Exception e) {
        log.warn(e, "Could not close aggregator [%s], skipping.", adapter.getFactory().getName());
      }
    }
  }

  private static class VectorAggregatorAdapter implements AggregatorAdapter
  {
    private final AggregatorFactory factory;
    private final VectorAggregator aggregator;

    public VectorAggregatorAdapter(final AggregatorFactory factory, final VectorAggregator aggregator)
    {
      this.factory = factory;
      this.aggregator = aggregator;
    }

    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      aggregator.init(buf, position);
    }

    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      return aggregator.get(buf, position);
    }

    @Override
    public void close()
    {
      aggregator.close();
    }

    @Override
    public void relocate(
        final int oldPosition,
        final int newPosition,
        final ByteBuffer oldBuffer,
        final ByteBuffer newBuffer
    )
    {
      aggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public AggregatorFactory getFactory()
    {
      return factory;
    }

    @Override
    public BufferAggregator asBufferAggregator()
    {
      throw new ISE("Not a BufferAggregator!");
    }

    @Override
    public VectorAggregator asVectorAggregator()
    {
      return aggregator;
    }
  }

  private static class BufferAggregatorAdapter implements AggregatorAdapter
  {
    private final AggregatorFactory factory;
    private final BufferAggregator aggregator;

    public BufferAggregatorAdapter(final AggregatorFactory factory, final BufferAggregator aggregator)
    {
      this.factory = factory;
      this.aggregator = aggregator;
    }

    @Override
    public void init(final ByteBuffer buf, final int position)
    {
      aggregator.init(buf, position);
    }

    @Override
    public Object get(final ByteBuffer buf, final int position)
    {
      return aggregator.get(buf, position);
    }

    @Override
    public void close()
    {
      aggregator.close();
    }

    @Override
    public void relocate(
        final int oldPosition,
        final int newPosition,
        final ByteBuffer oldBuffer,
        final ByteBuffer newBuffer
    )
    {
      aggregator.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public AggregatorFactory getFactory()
    {
      return factory;
    }

    @Override
    public BufferAggregator asBufferAggregator()
    {
      return aggregator;
    }

    @Override
    public VectorAggregator asVectorAggregator()
    {
      throw new ISE("Not a VectorAggregator!");
    }
  }
}
