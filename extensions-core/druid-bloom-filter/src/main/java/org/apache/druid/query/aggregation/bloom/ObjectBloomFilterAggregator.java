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
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import java.nio.ByteBuffer;

/**
 * Handles "unknown" columns by examining what comes out of the selector
 */
class ObjectBloomFilterAggregator extends BaseBloomFilterAggregator<BaseObjectColumnValueSelector<Object>>
{
  ObjectBloomFilterAggregator(
      BaseObjectColumnValueSelector<Object> selector,
      int maxNumEntries,
      boolean onHeap
  )
  {
    super(selector, maxNumEntries, onHeap);
  }

  @Override
  void bufferAdd(ByteBuffer buf)
  {
    final Object object = selector.getObject();
    if (object instanceof ByteBuffer) {
      final ByteBuffer other = (ByteBuffer) object;
      BloomKFilter.mergeBloomFilterByteBuffers(buf, buf.position(), other, other.position());
    } else {
      if (object instanceof Long) {
        BloomKFilter.addLong(buf, (long) object);
      } else if (object instanceof Double) {
        BloomKFilter.addDouble(buf, (double) object);
      } else if (object instanceof Float) {
        BloomKFilter.addFloat(buf, (float) object);
      } else if (object instanceof String) {
        BloomKFilter.addString(buf, (String) object);
      } else {
        BloomKFilter.addBytes(buf, null, 0, 0);
      }
    }
  }
}
