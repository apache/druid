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

import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;

public final class StringBloomFilterAggregator extends BaseBloomFilterAggregator<DimensionSelector>
{
  StringBloomFilterAggregator(DimensionSelector selector, int maxNumEntries, boolean onHeap)
  {
    super(selector, maxNumEntries, onHeap);
  }

  @Override
  public void bufferAdd(ByteBuffer buf)
  {
    stringBufferAdd(buf, selector);
  }

  static void stringBufferAdd(ByteBuffer buf, DimensionSelector selector)
  {
    if (selector.getRow().size() > 1) {
      selector.getRow().forEach(v -> {
        String value = selector.lookupName(v);
        if (value == null) {
          BloomKFilter.addBytes(buf, null, 0, 0);
        } else {
          BloomKFilter.addString(buf, value);
        }
      });
    } else {
      String value = (String) selector.getObject();
      if (value == null) {
        BloomKFilter.addBytes(buf, null, 0, 0);
      } else {
        BloomKFilter.addString(buf, value);
      }
    }
  }
}
