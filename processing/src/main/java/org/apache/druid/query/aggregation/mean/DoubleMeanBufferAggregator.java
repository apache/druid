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

package org.apache.druid.query.aggregation.mean;

import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class DoubleMeanBufferAggregator implements BufferAggregator
{

  private final ColumnValueSelector selector;

  public DoubleMeanBufferAggregator(ColumnValueSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    DoubleMeanHolder.init(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    Object update = selector.getObject();

    if (update instanceof DoubleMeanHolder) {
      DoubleMeanHolder.update(buf, position, (DoubleMeanHolder) update);
    } else if (update instanceof List) {
      for (Object o : (List) update) {
        DoubleMeanHolder.update(buf, position, Numbers.tryParseDouble(o, 0));
      }
    } else {
      DoubleMeanHolder.update(buf, position, Numbers.tryParseDouble(update, 0));
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return DoubleMeanHolder.get(buf, position);
  }

  @Override
  public float getFloat(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public long getLong(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public double getDouble(ByteBuffer buf, int position)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void close()
  {

  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
  }
}
