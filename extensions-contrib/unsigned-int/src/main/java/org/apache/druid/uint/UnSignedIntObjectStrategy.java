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

package org.apache.druid.uint;

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class UnSignedIntObjectStrategy implements ObjectStrategy<Long>
{
  @Override
  public Class<Long> getClazz()
  {
    return Long.class;
  }

  @Nullable
  @Override
  public Long fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes <= 0) {
      return null;
    }
    byte[] cache = new byte[numBytes];
    buffer.get(cache);
    ByteBuffer result = ByteBuffer.allocate(8).put(new byte[]{0, 0, 0, 0}).put(cache);
    result.position(0);
    long toReturn = result.getLong();
    return toReturn;

  }

  @Nullable
  @Override
  public byte[] toBytes(@Nullable Long val)
  {
    if (val == null) {
      return new byte[0];
    }
    byte[] bytes = new byte[8];
    ByteBuffer.wrap(bytes).putLong(val);
    return Arrays.copyOfRange(bytes, 4, 8);
  }

  @Override
  public int compare(Long left, Long right)
  {
    return left.compareTo(right);
  }
}
