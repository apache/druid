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

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class NoopVectorAggregator implements VectorAggregator
{
  private static final NoopVectorAggregator INSTANCE = new NoopVectorAggregator();

  public static NoopVectorAggregator instance()
  {
    return INSTANCE;
  }

  private NoopVectorAggregator()
  {
    // Singleton.
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    // Do nothing.
  }

  @Override
  public void aggregate(final ByteBuffer buf, final int position, final int startRow, final int endRow)
  {
    // Do nothing.
  }

  @Override
  public void aggregate(
      final ByteBuffer buf,
      final int numRows,
      final int[] positions,
      @Nullable final int[] rows,
      final int positionOffset
  )
  {
    // Do nothing.
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return null;
  }

  @Override
  public void close()
  {
    // Do nothing.
  }
}
