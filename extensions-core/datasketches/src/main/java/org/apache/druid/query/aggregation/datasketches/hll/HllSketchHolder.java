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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

public class HllSketchHolder
{
  public static HllSketchHolder fromObj(Object obj)
  {
    if (obj == null) {
      throw new NullPointerException("HllSketchHolder.fromObj cannot take a null argument");
    }

    if (obj instanceof HllSketchHolder) {
      return (HllSketchHolder) obj;
    } else if (obj instanceof HllSketch) {
      return HllSketchHolder.of((HllSketch) obj);
    } else if (obj instanceof Union) {
      return HllSketchHolder.of((Union) obj);
    } else if (obj instanceof byte[]) {
      return HllSketchHolder.of(HllSketch.heapify((byte[]) obj));
    } else if (obj instanceof Memory) {
      return HllSketchHolder.of(HllSketch.wrap((Memory) obj));
    } else if (obj instanceof String) {
      return HllSketchHolder.of(HllSketch.heapify(StringUtils.decodeBase64(StringUtils.toUtf8((String) obj))));
    }

    throw new ISE("Object is not of a type[%s] that can be deserialized to sketch.", obj.getClass());
  }

  public static HllSketchHolder of(Union union)
  {
    return new HllSketchHolder(union, null);
  }

  public static HllSketchHolder of(HllSketch sketch)
  {
    return new HllSketchHolder(null, sketch);
  }

  private Union union;
  private HllSketch sketch;

  public HllSketchHolder(
      Union union,
      HllSketch sketch
  )
  {
    this.union = union;
    this.sketch = sketch;

    if (this.union == null && this.sketch == null) {
      throw new ISE("Both union and sketch were null!");
    }
  }

  @JsonValue
  public HllSketch getSketch()
  {
    if (sketch == null) {
      sketch = union.getResult();
    }

    return sketch;
  }

  public HllSketch getSketch(TgtHllType type)
  {
    if (sketch == null) {
      sketch = union.getResult(type);
    }

    if (sketch.getTgtHllType() != type) {
      sketch = sketch.copyAs(type);
    }

    return sketch;
  }

  public void add(HllSketch sketchToAdd)
  {
    if (union == null) {
      union = new Union(sketchToAdd.getLgConfigK());
      union.update(this.sketch);
    }

    union.update(sketchToAdd);
    sketch = null;
  }

  public double getEstimate()
  {
    if (sketch != null) {
      return sketch.getEstimate();
    } else if (union != null) {
      return union.getEstimate();
    }
    return 0;
  }

  public double getLowerBound(int stdDevs)
  {
    if (sketch != null) {
      return sketch.getLowerBound(stdDevs);
    } else if (union != null) {
      return union.getLowerBound(stdDevs);
    }
    return 0;
  }

  public double getUpperBound(int stdDevs)
  {
    if (sketch != null) {
      return sketch.getUpperBound(stdDevs);
    } else if (union != null) {
      return union.getUpperBound(stdDevs);
    }
    return 0;
  }

  @SuppressWarnings("VariableNotUsedInsideIf")
  public HllSketchHolder merge(HllSketchHolder other)
  {
    // It appears like we could make this code cleaner by checking for other.union first and then delegating to add
    // if it's not.  But, we check ourselves first because callers would tend to expect that the object they are
    // calling a method on is more likely to get mutated, so we prefer to check and merge into this object if
    // possible first.
    if (union == null) {
      if (other.union == null) {
        add(other.getSketch());
        return this;
      } else {
        other.add(sketch);
        return other;
      }
    } else {
      add(other.getSketch());
      return this;
    }
  }
}
