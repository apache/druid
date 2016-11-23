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

package io.druid.query.aggregation.datasketches.quantiles;

import com.google.common.base.Charsets;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import org.apache.commons.codec.binary.Base64;

public class QuantilesSketchUtils
{
  public final static int MIN_K = 16;

  public static DoublesSketch deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return deserializeFromBase64EncodedString((String) serializedSketch);
    } else if (serializedSketch instanceof byte[]) {
      return deserializeFromByteArray((byte[]) serializedSketch);
    } else if (serializedSketch instanceof DoublesSketch) {
      return (DoublesSketch) serializedSketch;
    }

    throw new IllegalStateException(
        "Object is not of a type that can deserialize to sketch: "
        + serializedSketch.getClass()
    );
  }

  public static DoublesSketch deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(
        Base64.decodeBase64(
            str.getBytes(Charsets.UTF_8)
        )
    );
  }

  public static DoublesSketch deserializeFromByteArray(byte[] data)
  {
    return deserializeFromMemory(new NativeMemory(data));
  }

  public static DoublesSketch deserializeFromMemory(Memory mem)
  {
    //should support "wrap" once off heap is supported
    return DoublesSketch.heapify(mem);
  }

  public static DoublesUnion buildUnion(int size)
  {
    return new DoublesUnionBuilder().setK(size).build();
  }

  public static DoublesSketch buildSketch(int size)
  {
    return new DoublesSketchBuilder().build(size);
  }
}
