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

package org.apache.druid.query.aggregation.cardinality;

import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.aggregation.cardinality.vector.CardinalityVectorProcessor;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesBufferAggregator;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class CardinalityVectorAggregator implements VectorAggregator
{
  private final List<CardinalityVectorProcessor> processors;

  CardinalityVectorAggregator(List<CardinalityVectorProcessor> processors)
  {
    this.processors = processors;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    HyperUniquesBufferAggregator.doInit(buf, position);
  }

  @Override
  public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
  {
    for (final CardinalityVectorProcessor processor : processors) {
      processor.aggregate(buf, position, startRow, endRow);
    }
  }

  @Override
  public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
  {
    for (final CardinalityVectorProcessor processor : processors) {
      processor.aggregate(buf, numRows, positions, rows, positionOffset);
    }
  }

  @Nullable
  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return HyperUniquesBufferAggregator.doGet(buf, position);
  }

  @Override
  public void close()
  {
    // Nothing to close.
  }
}
