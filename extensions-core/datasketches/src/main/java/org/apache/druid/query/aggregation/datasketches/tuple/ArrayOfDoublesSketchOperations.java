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

package org.apache.druid.query.aggregation.datasketches.tuple;

import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.tuple.ArrayOfDoublesAnotB;
import org.apache.datasketches.tuple.ArrayOfDoublesCombiner;
import org.apache.datasketches.tuple.ArrayOfDoublesIntersection;
import org.apache.datasketches.tuple.ArrayOfDoublesSetOperationBuilder;
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesSketches;
import org.apache.datasketches.tuple.ArrayOfDoublesUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.charset.StandardCharsets;

public class ArrayOfDoublesSketchOperations
{

  public enum Operation
  {
    UNION {
      @Override
      public ArrayOfDoublesSketch apply(final int nominalEntries, final int numberOfValues, final ArrayOfDoublesSketch[] sketches)
      {
        final ArrayOfDoublesUnion union = new ArrayOfDoublesSetOperationBuilder().setNominalEntries(nominalEntries)
            .setNumberOfValues(numberOfValues).buildUnion();
        for (final ArrayOfDoublesSketch sketch : sketches) {
          union.update(sketch);
        }
        return union.getResult();
      }
    },
    INTERSECT {
      @Override
      public ArrayOfDoublesSketch apply(final int nominalEntries, final int numberOfValues, final ArrayOfDoublesSketch[] sketches)
      {
        final ArrayOfDoublesIntersection intersection = new ArrayOfDoublesSetOperationBuilder()
            .setNominalEntries(nominalEntries).setNumberOfValues(numberOfValues).buildIntersection();
        for (final ArrayOfDoublesSketch sketch : sketches) {
          intersection.update(sketch, COMBINER);
        }
        return intersection.getResult();
      }
    },
    NOT {
      @Override
      public ArrayOfDoublesSketch apply(final int nominalEntries, final int numberOfValues, final ArrayOfDoublesSketch[] sketches)
      {
        if (sketches.length < 1) {
          throw new IAE("A-Not-B requires at least 1 sketch");
        }

        if (sketches.length == 1) {
          return sketches[0];
        }

        ArrayOfDoublesSketch result = sketches[0];
        for (int i = 1; i < sketches.length; i++) {
          final ArrayOfDoublesAnotB aNotB = new ArrayOfDoublesSetOperationBuilder().setNumberOfValues(numberOfValues)
              .buildAnotB();
          aNotB.update(result, sketches[i]);
          result = aNotB.getResult();
        }
        return result;
      }
    };

    public abstract ArrayOfDoublesSketch apply(int nominalEntries, int numberOfValues, ArrayOfDoublesSketch[] sketches);
  }

  // This is how to combine values for sketch intersection.
  // Might not fit all use cases.
  private static ArrayOfDoublesCombiner COMBINER = new ArrayOfDoublesCombiner()
  {
    @Override
    public double[] combine(final double[] a, final double[] b)
    {
      final double[] result = new double[a.length];
      for (int i = 0; i < a.length; i++) {
        result[i] = a[i] + b[i];
      }
      return result;
    }
  };

  public static ArrayOfDoublesSketch deserialize(final Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return deserializeFromBase64EncodedString((String) serializedSketch);
    } else if (serializedSketch instanceof byte[]) {
      return deserializeFromByteArray((byte[]) serializedSketch);
    } else if (serializedSketch instanceof ArrayOfDoublesSketch) {
      return (ArrayOfDoublesSketch) serializedSketch;
    }
    throw new ISE("Object is not of a type that can deserialize to sketch: %s", serializedSketch.getClass());
  }

  public static ArrayOfDoublesSketch deserializeFromBase64EncodedString(final String str)
  {
    return deserializeFromByteArray(StringUtils.decodeBase64(str.getBytes(StandardCharsets.UTF_8)));
  }

  public static ArrayOfDoublesSketch deserializeFromByteArray(final byte[] data)
  {
    final Memory mem = Memory.wrap(data);
    return ArrayOfDoublesSketches.wrapSketch(mem);
  }

}
