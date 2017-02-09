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

package io.druid.query.groupby.epinephelinae.column;

import io.druid.query.ColumnSelectorPlus;

public class GroupByColumnSelectorPlus extends ColumnSelectorPlus<GroupByColumnSelectorStrategy>
{
  /**
   * Indicates the offset of this dimension's value within the grouping key.
   */
  private int keyBufferPosition;

  public GroupByColumnSelectorPlus(ColumnSelectorPlus<GroupByColumnSelectorStrategy> baseInfo, int keyBufferPosition)
  {
    super(
        baseInfo.getName(),
        baseInfo.getOutputName(),
        baseInfo.getColumnSelectorStrategy(),
        baseInfo.getSelector()
    );
    this.keyBufferPosition = keyBufferPosition;
  }

  public int getKeyBufferPosition()
  {
    return keyBufferPosition;
  }
}
