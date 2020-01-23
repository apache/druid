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

package org.apache.druid.query.groupby.epinephelinae.vector;

import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import java.nio.ByteBuffer;

public class SingleValueStringGroupByVectorColumnSelector implements GroupByVectorColumnSelector
{
  private final SingleValueDimensionVectorSelector selector;

  SingleValueStringGroupByVectorColumnSelector(final SingleValueDimensionVectorSelector selector)
  {
    this.selector = selector;
  }

  @Override
  public int getGroupingKeySize()
  {
    return 1;
  }

  @Override
  public void writeKeys(
      final int[] keySpace,
      final int keySize,
      final int keyOffset,
      final int startRow,
      final int endRow
  )
  {
    final int[] rowVector = selector.getRowVector();

    for (int i = startRow, j = keyOffset; i < endRow; i++, j += keySize) {
      keySpace[j] = rowVector[i];
    }
  }

  @Override
  public void writeKeyToResultRow(
      final ByteBuffer keyBuffer,
      final int keyOffset,
      final ResultRow resultRow,
      final int resultRowPosition
  )
  {
    final int id = keyBuffer.getInt(keyOffset * Integer.BYTES);
    resultRow.set(resultRowPosition, selector.lookupName(id));
  }
}
