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

package org.apache.druid.query.aggregation.ddsketch;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.store.CollapsingLowestDenseStore;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class DDSketchObjectStrategy implements ObjectStrategy<DDSketch>
{
  private static final byte[] EMPTY_BYTES = new byte[0];

  @Override
  public Class<DDSketch> getClazz()
  {
    return DDSketch.class;
  }

  @Override
  public DDSketch fromByteBuffer(ByteBuffer buffer, int numBytes)
  {
    if (numBytes == 0) {
      return null;
    }
    ByteBuffer readOnlyBuffer = buffer.asReadOnlyBuffer();
    readOnlyBuffer.limit(buffer.position() + numBytes);
    try {
      com.datadoghq.sketch.ddsketch.proto.DDSketch proto = com.datadoghq.sketch.ddsketch.proto.DDSketch.parseFrom(readOnlyBuffer);
      DDSketch recovered = DDSketchProtoBinding.fromProto(() -> new CollapsingLowestDenseStore(1000), proto);
      return recovered;
    } 
    catch (InvalidProtocolBufferException e) {
      throw new UnsupportedOperationException("Unable to decode from Proto");
    }
  }

  @Override
  public byte[] toBytes(@Nullable DDSketch val)
  {
    if (val == null) {
      return EMPTY_BYTES;
    }
    return DDSketchProtoBinding.toProto(val).toByteArray();
  }

  @Override
  public int compare(DDSketch o1, DDSketch o2)
  {
    return DDSketchAggregatorFactory.COMPARATOR.compare(o1, o2);
  }
}
