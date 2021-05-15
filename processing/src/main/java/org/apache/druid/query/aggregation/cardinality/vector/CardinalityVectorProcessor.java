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

package org.apache.druid.query.aggregation.cardinality.vector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Processor for {@link org.apache.druid.query.aggregation.cardinality.CardinalityVectorAggregator}.
 */
public interface CardinalityVectorProcessor
{
  /**
   * Processor for {@link org.apache.druid.query.aggregation.VectorAggregator#aggregate(ByteBuffer, int, int, int)}
   * in byRow = false mode.
   */
  void aggregate(ByteBuffer buf, int position, int startRow, int endRow);

  /**
   * Processor for {@link org.apache.druid.query.aggregation.VectorAggregator#aggregate(ByteBuffer, int, int, int)}
   * in byRow = false mode.
   */
  void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset);
}
