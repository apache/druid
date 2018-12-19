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

package io.druid.query.aggregation.unique;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import me.lemire.integercompression.differential.IntegratedIntCompressor;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class RoaringBitmapSerializer extends JsonSerializer<ImmutableRoaringBitmap>
{
  @Override
  public void serialize(ImmutableRoaringBitmap value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException
  {
    final int size = value.getCardinality();
    if (size <= 1000000) {
      if (value instanceof MutableRoaringBitmap) {
        ((MutableRoaringBitmap) value).runOptimize();
      }
      try (final ByteArrayOutputStream out = new ByteArrayOutputStream();
           final DataOutputStream data = new DataOutputStream(out)) {
        value.serialize(data);
        jgen.writeBinary(out.toByteArray());
      }
    } else {
      IntegratedIntCompressor iic = new IntegratedIntCompressor();
      int[] compressed = iic.compress(value.toArray());
      jgen.writeStartArray();
      final int length = compressed.length;
      for (int i = 0; i < length; i++) {
        jgen.writeNumber(compressed[i]);
      }
      jgen.writeEndArray();
    }
  }
}
