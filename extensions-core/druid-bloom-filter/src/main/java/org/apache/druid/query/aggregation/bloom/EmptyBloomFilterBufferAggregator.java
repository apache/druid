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

import org.apache.druid.segment.NilColumnValueSelector;

import java.nio.ByteBuffer;

public final class EmptyBloomFilterBufferAggregator extends BaseBloomFilterBufferAggregator<NilColumnValueSelector>
{
  EmptyBloomFilterBufferAggregator(int maxNumEntries)
  {
    super(NilColumnValueSelector.instance(), maxNumEntries);
  }

  @Override
  public void bufferAdd(ByteBuffer buf)
  {
    // nothing to do
  }

  @Override
  public void aggregate(ByteBuffer buf, int position)
  {
    // nothing to do
  }
}
