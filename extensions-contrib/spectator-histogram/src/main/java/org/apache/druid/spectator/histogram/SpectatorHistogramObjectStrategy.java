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

package org.apache.druid.spectator.histogram;

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class SpectatorHistogramObjectStrategy implements ObjectStrategy<SpectatorHistogram>
{
  private static final byte[] EMPTY_BYTES = null;

  @Override
  public Class<SpectatorHistogram> getClazz()
  {
    return SpectatorHistogram.class;
  }

  @Override
  public SpectatorHistogram fromByteBuffer(ByteBuffer readOnlyBuffer, int numBytes)
  {
    if (numBytes == 0) {
      return null;
    }
    return SpectatorHistogram.fromByteBuffer(readOnlyBuffer);
  }

  @Override
  public byte[] toBytes(@Nullable SpectatorHistogram val)
  {
    if (val == null) {
      return EMPTY_BYTES;
    }
    return val.toBytes();
  }

  @Override
  public int compare(SpectatorHistogram o1, SpectatorHistogram o2)
  {
    return SpectatorHistogramAggregatorFactory.COMPARATOR.compare(o1, o2);
  }
}
