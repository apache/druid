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

package io.druid.query.aggregation.datasketches.theta;

import com.google.common.base.Charsets;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.memory.Memory;
import com.yahoo.sketches.memory.NativeMemory;
import com.yahoo.sketches.theta.AnotB;
import com.yahoo.sketches.theta.Intersection;
import com.yahoo.sketches.theta.SetOperation;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.Union;

import io.druid.java.util.common.logger.Logger;

import org.apache.commons.codec.binary.Base64;

public class SketchOperations
{

  private static final Logger LOG = new Logger(SketchOperations.class);

  public static final Sketch EMPTY_SKETCH = Sketches.updateSketchBuilder().build().compact(true, null);

  public static enum Func
  {
    UNION,
    INTERSECT,
    NOT;
  }

  public static Sketch deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return deserializeFromBase64EncodedString((String) serializedSketch);
    } else if (serializedSketch instanceof byte[]) {
      return deserializeFromByteArray((byte[]) serializedSketch);
    } else if (serializedSketch instanceof Sketch) {
      return (Sketch) serializedSketch;
    }

    throw new IllegalStateException(
        "Object is not of a type that can deserialize to sketch: "
        + serializedSketch.getClass()
    );
  }

  public static Sketch deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(
        Base64.decodeBase64(
            str.getBytes(Charsets.UTF_8)
        )
    );
  }

  public static Sketch deserializeFromByteArray(byte[] data)
  {
    return deserializeFromMemory(new NativeMemory(data));
  }

  public static Sketch deserializeFromMemory(Memory mem)
  {
    if (Sketch.getSerializationVersion(mem) < 3) {
      return Sketches.heapifySketch(mem);
    } else {
      return Sketches.wrapSketch(mem);
    }
  }

  public static Sketch sketchSetOperation(Func func, int sketchSize, Sketch... sketches)
  {
    //in the code below, I am returning SetOp.getResult(false, null)
    //"false" gets us an unordered sketch which is faster to build
    //"true" returns an ordered sketch but slower to compute. advantage of ordered sketch
    //is that they are faster to "union" later but given that this method is used in
    //the final stages of query processing, ordered sketch would be of no use.
    switch (func) {
      case UNION:
        Union union = (Union) SetOperation.builder().build(sketchSize, Family.UNION);
        for (Sketch sketch : sketches) {
          union.update(sketch);
        }
        return union.getResult(false, null);
      case INTERSECT:
        Intersection intersection = (Intersection) SetOperation.builder().build(sketchSize, Family.INTERSECTION);
        for (Sketch sketch : sketches) {
          intersection.update(sketch);
        }
        return intersection.getResult(false, null);
      case NOT:
        if (sketches.length < 1) {
          throw new IllegalArgumentException("A-Not-B requires atleast 1 sketch");
        }

        if (sketches.length == 1) {
          return sketches[0];
        }

        Sketch result = sketches[0];
        for (int i = 1; i < sketches.length; i++) {
          AnotB anotb = (AnotB) SetOperation.builder().build(sketchSize, Family.A_NOT_B);
          anotb.update(result, sketches[i]);
          result = anotb.getResult(false, null);
        }
        return result;
      default:
        throw new IllegalArgumentException("Unknown sketch operation " + func);
    }
  }
}
