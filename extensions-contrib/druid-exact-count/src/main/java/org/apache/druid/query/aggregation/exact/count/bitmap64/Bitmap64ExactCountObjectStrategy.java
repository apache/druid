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

package org.apache.druid.query.aggregation.exact.count.bitmap64;

import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

public class Bitmap64ExactCountObjectStrategy implements ObjectStrategy<Bitmap64>
{

  static final Bitmap64ExactCountObjectStrategy STRATEGY = new Bitmap64ExactCountObjectStrategy();

  @Override
  public Class<? extends Bitmap64> getClazz()
  {
    return RoaringBitmap64Counter.class;
  }

  @Nullable
  @Override
  public Bitmap64 fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    final ByteBuffer readOnlyBuf = buffer.asReadOnlyBuffer();

    if (readOnlyBuf.remaining() < numBytes) {
      throw new BufferUnderflowException();
    }

    if (readOnlyBuf.hasArray()) {
      // Use the underlying array directly without copying the entire byte array into input stream.
      return RoaringBitmap64Counter.fromDataInput(new DataInputStream(
          new ByteArrayInputStream(readOnlyBuf.array(), readOnlyBuf.arrayOffset() + readOnlyBuf.position(), numBytes)));
    } else {
      byte[] bytes = new byte[numBytes];
      readOnlyBuf.get(bytes, 0, numBytes);
      return RoaringBitmap64Counter.fromBytes(bytes);
    }
  }

  @Nullable
  @Override
  public byte[] toBytes(@Nullable Bitmap64 val)
  {
    if (val == null) {
      return new byte[0];
    }
    return val.toByteBuffer().array();
  }

  @Override
  public int compare(Bitmap64 o1, Bitmap64 o2)
  {
    return Bitmap64ExactCountAggregatorFactory.COMPARATOR.compare(o1, o2);
  }
}
