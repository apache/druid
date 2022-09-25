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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;

/**
 * Defines strategy on how to read and write data from deep storage.
 */
public class CompressedBigDecimalObjectStrategy implements ObjectStrategy<CompressedBigDecimal>
{

  /*
   * (non-Javadoc)
   *
   * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public int compare(CompressedBigDecimal o1, CompressedBigDecimal o2)
  {
    return o1.compareTo(o2);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.druid.segment.data.ObjectStrategy#getClazz()
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public Class getClazz()
  {
    return CompressedBigDecimal.class;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.druid.segment.data.ObjectStrategy#fromByteBuffer(java.nio.ByteBuffer, int)
   */
  @Override
  public CompressedBigDecimal fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    ByteBuffer myBuf = buffer.slice();
    myBuf.order(ByteOrder.LITTLE_ENDIAN);
    IntBuffer intBuf = myBuf.asIntBuffer();
    int scale = intBuf.get();
    int[] array = new int[numBytes / 4 - 1];
    intBuf.get(array);

    return ArrayCompressedBigDecimal.wrap(array, scale);
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.druid.segment.data.ObjectStrategy#toBytes(java.lang.Object)
   */
  @Override
  public byte[] toBytes(CompressedBigDecimal val)
  {
    ByteBuffer buf = ByteBuffer.allocate(4 * (val.getArraySize() + 1));
    buf.order(ByteOrder.LITTLE_ENDIAN);
    IntBuffer intBuf = buf.asIntBuffer();
    intBuf.put(val.getScale());
    for (int ii = 0; ii < val.getArraySize(); ++ii) {
      intBuf.put(val.getArrayEntry(ii));
    }

    return buf.array();
  }
}
