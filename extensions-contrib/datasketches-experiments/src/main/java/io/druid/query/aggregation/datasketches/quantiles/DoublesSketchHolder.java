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
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.yahoo.memory.Memory;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.quantiles.DoublesSketch;
import com.yahoo.sketches.quantiles.DoublesSketchBuilder;
import com.yahoo.sketches.quantiles.DoublesUnion;
import com.yahoo.sketches.quantiles.DoublesUnionBuilder;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import org.apache.commons.codec.binary.Base64;

import java.util.Comparator;

/**
 */
public class DoublesSketchHolder
{
  public static final DoublesSketchHolder EMPTY = DoublesSketchHolder.of(
      DoublesSketch.builder().build()
  );

  public static final Comparator<Object> COMPARATOR = Ordering.from(
      new Comparator()
      {
        @Override
        public int compare(Object o1, Object o2)
        {
          DoublesSketchHolder h1 = (DoublesSketchHolder) o1;
          DoublesSketchHolder h2 = (DoublesSketchHolder) o2;

          if (h1.obj instanceof DoublesSketch || h1.obj instanceof DoublesUnion) {
            if (h2.obj instanceof DoublesSketch || h2.obj instanceof DoublesUnion) {
              return SKETCH_COMPARATOR.compare(h1.getSketch(), h2.getSketch());
            } else {
              return -1;
            }
          }

          if (h1.obj instanceof Memory) {
            if (h2.obj instanceof Memory) {
              return MEMORY_COMPARATOR.compare((Memory) h1.obj, (Memory) h2.obj);
            } else {
              return 1;
            }
          }

          throw new IAE("Unknwon types [%s] and [%s]", h1.obj.getClass().getName(), h2.obj.getClass().getName());
        }
      }
  ).nullsFirst();

  private static final Comparator<DoublesSketch> SKETCH_COMPARATOR = new Comparator<DoublesSketch>()
  {
    @Override
    public int compare(DoublesSketch o1, DoublesSketch o2)
    {
      return Doubles.compare(o1.getMinValue(), o2.getMinValue());
    }
  };

  private static final Comparator<Memory> MEMORY_COMPARATOR = new Comparator<Memory>()
  {
    @Override
    public int compare(Memory o1, Memory o2)
    {
      // We have two Ordered Compact sketches, so just compare their last entry if they have the size.
      // This is to produce a deterministic ordering, though it might not match the actual estimate
      // ordering, but that's ok because this comparator is only used by GenericIndexed
      int retVal = Longs.compare(o1.getCapacity(), o2.getCapacity());
      if (retVal == 0) {
        retVal = Longs.compare(o1.getLong(o2.getCapacity() - 8), o2.getLong(o2.getCapacity() - 8));
      }

      return retVal;
    }
  };


  private final Object obj;

  private volatile DoublesSketch cachedSketch = null;

  private DoublesSketchHolder(Object obj)
  {
    Preconditions.checkArgument(
        obj instanceof DoublesSketch || obj instanceof DoublesUnion || obj instanceof Memory,
        "unknown sketch representation type [%s]", obj.getClass().getName()
    );
    this.obj = obj;
  }

  public static DoublesSketchHolder of(Object obj)
  {
    return new DoublesSketchHolder(obj);
  }

  public void updateUnion(DoublesUnion union)
  {
    if (obj instanceof Memory) {
      union.update((Memory) obj);
    } else {
      union.update(getSketch());
    }
  }

  public DoublesSketch getSketch()
  {
    if (cachedSketch != null) {
      return cachedSketch;
    }

    if (obj instanceof DoublesSketch) {
      cachedSketch = (DoublesSketch) obj;
    } else if (obj instanceof DoublesUnion) {
      cachedSketch = ((DoublesUnion) obj).getResult();
    } else if (obj instanceof Memory) {
      cachedSketch = deserializeFromMemory((Memory) obj);
    } else {
      throw new ISE("Can't get sketch from object of type [%s]", obj.getClass().getName());
    }
    return cachedSketch;
  }

  public static Object combine(Object o1, Object o2, int nomEntries)
  {
    DoublesSketchHolder holder1 = (DoublesSketchHolder) o1;
    DoublesSketchHolder holder2 = (DoublesSketchHolder) o2;

    if (holder1.obj instanceof DoublesUnion) {
      DoublesUnion union = (DoublesUnion) holder1.obj;
      holder2.updateUnion(union);
      holder1.invalidateCache();
      return holder1;
    } else if (holder2.obj instanceof DoublesUnion) {
      DoublesUnion union = (DoublesUnion) holder2.obj;
      holder1.updateUnion(union);
      holder2.invalidateCache();
      return holder2;
    } else {
      DoublesUnion union = buildUnion(nomEntries);
      holder1.updateUnion(union);
      holder2.updateUnion(union);
      return DoublesSketchHolder.of(union);
    }
  }

  private void invalidateCache()
  {
    cachedSketch = null;
  }

  public static DoublesSketchHolder deserialize(Object serializedSketch)
  {
    if (serializedSketch instanceof String) {
      return DoublesSketchHolder.of(deserializeFromBase64EncodedString((String) serializedSketch));
    } else if (serializedSketch instanceof byte[]) {
      return DoublesSketchHolder.of(deserializeFromByteArray((byte[]) serializedSketch));
    } else if (serializedSketch instanceof DoublesSketchHolder) {
      return (DoublesSketchHolder) serializedSketch;
    } else if (serializedSketch instanceof DoublesSketch
               || serializedSketch instanceof DoublesUnion
               || serializedSketch instanceof Memory) {
      return DoublesSketchHolder.of(serializedSketch);
    }

    throw new ISE(
        "Object is not of a type[%s] that can be deserialized to sketch.",
        serializedSketch.getClass()
    );
  }

  private static DoublesSketch deserializeFromBase64EncodedString(String str)
  {
    return deserializeFromByteArray(
        Base64.decodeBase64(
            str.getBytes(Charsets.UTF_8)
        )
    );
  }

  private static DoublesSketch deserializeFromByteArray(byte[] data)
  {
    return deserializeFromMemory(new NativeMemory(data));
  }

  private static DoublesSketch deserializeFromMemory(Memory mem)
  {
    return DoublesSketch.wrap(mem);
  }

  public static DoublesUnion buildUnion(int size)
  {
    return new DoublesUnionBuilder().setMaxK(size).build();
  }

  public static DoublesSketch buildSketch(int size)
  {
    return new DoublesSketchBuilder().build(size);
  }
}
