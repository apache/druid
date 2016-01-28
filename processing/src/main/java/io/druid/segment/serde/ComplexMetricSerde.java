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

package io.druid.segment.serde;

import com.google.common.base.Function;
import io.druid.segment.column.ColumnBuilder;
import io.druid.segment.data.ObjectStrategy;

import java.nio.ByteBuffer;

/**
 */
public abstract class ComplexMetricSerde
{
  public abstract String getTypeName();
  public abstract ComplexMetricExtractor getExtractor();

  /**
   * Deserializes a ByteBuffer and adds it to the ColumnBuilder.  This method allows for the ComplexMetricSerde
   * to implement it's own versioning scheme to allow for changes of binary format in a forward-compatible manner.
   *
   * @param buffer the buffer to deserialize
   * @param builder ColumnBuilder to add the column to
   */
  public abstract void deserializeColumn(ByteBuffer buffer, ColumnBuilder builder);

  /**
   * This is deprecated because its usage is going to be removed from the code.
   *
   * It was introduced before deserializeColumn() existed.  This method creates the assumption that Druid knows
   * how to interpret the actual column representation of the data, but I would much prefer that the ComplexMetricSerde
   * objects be in charge of creating and interpreting the whole column, which is what deserializeColumn lets
   * them do.
   *
   * @return an ObjectStrategy as used by GenericIndexed
   */
  @Deprecated
  public abstract ObjectStrategy getObjectStrategy();

  /**
   * Returns a function that can convert the Object provided by the ComplexColumn created through deserializeColumn
   * into a number of expected input bytes to produce that object.
   *
   * This is used to approximate the size of the input data via the SegmentMetadataQuery and does not need to be
   * overridden if you do not care about the query.
   *
   * @return A function that can compute the size of the complex object or null if you cannot/do not want to compute it
   */
  public Function<Object, Long> inputSizeFn()
  {
    return null;
  }

  /**
   * Converts intermediate representation of aggregate to byte[].
   *
   * @param val intermediate representation of aggregate
   * @return serialized intermediate representation of aggregate in byte[]
   */
  public byte[] toBytes(Object val)
  {
    return getObjectStrategy().toBytes(val);
  }

  /**
   * Converts byte[] to intermediate representation of the aggregate.
   *
   * @param byte array
   * @param start offset in the byte array where to start reading
   * @param numBytes number of bytes to read in given array
   * @return intermediate representation of the aggregate
   */
  public Object fromBytes(byte[] data, int start, int numBytes)
  {
    ByteBuffer bb = ByteBuffer.wrap(data);
    if(start > 0) {
      bb.position(start);
    }
    return getObjectStrategy().fromByteBuffer(bb, numBytes);
  }
}
