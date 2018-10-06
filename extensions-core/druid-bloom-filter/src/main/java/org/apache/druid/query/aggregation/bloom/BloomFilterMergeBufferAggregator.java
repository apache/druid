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

package org.apache.druid.query.aggregation.bloom;

import com.fasterxml.jackson.databind.util.ByteBufferBackedOutputStream;
import org.apache.druid.io.ByteBufferInputStream;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.hive.common.util.BloomKFilter;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BloomFilterMergeBufferAggregator implements BufferAggregator
{
  private final ColumnValueSelector<BloomKFilter> selector;
  private final int maxNumEntries;

  public BloomFilterMergeBufferAggregator(ColumnValueSelector<BloomKFilter> selector, int maxNumEntries)
  {
    this.selector = selector;
    this.maxNumEntries = maxNumEntries;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    final ByteBuffer mutationBuffer = buf.duplicate();
    mutationBuffer.position(position);
    BloomKFilter filter = new BloomKFilter(maxNumEntries);
    ByteBufferBackedOutputStream outputStream = new ByteBufferBackedOutputStream(mutationBuffer);
    try {
      BloomKFilter.serialize(outputStream, filter);
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to initialize bloomK filter", ex);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    final int oldPosition = buf.position();
    final int oldLimit = buf.limit();
    try {
      buf.position(position);
      BloomKFilter collector = BloomKFilter.deserialize(new ByteBufferInputStream(buf));
      BloomKFilter other = selector.getObject();
      if (other != null) {
        collector.merge(other);
        buf.position(position);
        ByteBufferBackedOutputStream out = new ByteBufferBackedOutputStream(buf);
        BloomKFilter.serialize(out, collector);
      }
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to merge bloomK filters", ex);
    }
    finally {
      buf.position(oldPosition);
      buf.limit(oldLimit);
    }
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    try {
      ByteBuffer mutationBuffer = buf.duplicate();
      mutationBuffer.position(position);
      return BloomKFilter.deserialize(new ByteBufferInputStream(mutationBuffer));
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to deserialize bloomK filter", ex);
    }
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterMergeBufferAggregator does not support getFloat()");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterMergeBufferAggregator does not support getLong()");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("BloomFilterMergeBufferAggregator does not support getDouble()");
  }

  @Override
  public void close()
  {
    // nothing to close
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
