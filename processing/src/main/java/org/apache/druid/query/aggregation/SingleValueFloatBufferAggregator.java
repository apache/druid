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

import org.apache.druid.segment.BaseFloatColumnValueSelector;

import java.nio.ByteBuffer;

/**
 */
public class SingleValueFloatBufferAggregator extends SingleValueBufferAggregator
{

  final BaseFloatColumnValueSelector selector;

  SingleValueFloatBufferAggregator(BaseFloatColumnValueSelector selector)
  {
    super();
    this.selector = selector;
  }

  @Override
  public void init(ByteBuffer buf, int position)
  {
    buf.putFloat(position, 0.0f);
  }

  @Override
  void updateBuffervalue(ByteBuffer buf, int position) {
    buf.putFloat(position, selector.getFloat());
  }

  @Override
  public Object get(ByteBuffer buf, int position)
  {
    return buf.getFloat(position);
  }

  @Override
  public String toString() {
    return "SingleValueFloatBufferAggregator{" +
            "selector=" + selector +
            ", aggregateInvoked=" + aggregateInvoked +
            '}';
  }
}
