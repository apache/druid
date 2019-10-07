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

package org.apache.druid.query.aggregation.tdigestsketch;

import com.tdunning.math.stats.MergingDigest;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class TDigestSketchObjectStrategy implements ObjectStrategy<MergingDigest>
{
  private static final byte[] EMPTY_BYTES = new byte[0];

  @Override
  public Class<MergingDigest> getClazz()
  {
    return MergingDigest.class;
  }

  @Override
  public MergingDigest fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0) {
      return null;
    }
    ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
    readOnlyBuffer.limit(buffer.position() + numBytes);
    return MergingDigest.fromBytes(readOnlyBuffer);
  }

  @Override
  public byte[] toBytes(@Nullable MergingDigest val)
  {
    if (val == null) {
      return EMPTY_BYTES;
    }
    return TDigestSketchUtils.toBytes(val);
  }

  @Override
  public int compare(MergingDigest o1, MergingDigest o2)
  {
    return TDigestSketchAggregatorFactory.COMPARATOR.compare(o1, o2);
  }
}
