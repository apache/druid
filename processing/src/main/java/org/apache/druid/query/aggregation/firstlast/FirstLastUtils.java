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

package org.apache.druid.query.aggregation.firstlast;

import org.apache.druid.query.aggregation.SerializablePairLongDouble;
import org.apache.druid.query.aggregation.SerializablePairLongFloat;
import org.apache.druid.query.aggregation.SerializablePairLongLong;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

public class FirstLastUtils
{

  /**
   * Returns whether a given value selector *might* contain object assignable from pairClass (SerializablePairLong*).
   */
  public static boolean selectorNeedsFoldCheck(
      final BaseObjectColumnValueSelector<?> valueSelector,
      @Nullable final ColumnCapabilities valueSelectorCapabilities,
      Class pairClass
  )
  {
    if (valueSelectorCapabilities != null && !valueSelectorCapabilities.is(ValueType.COMPLEX)) {
      // Known, non-complex type.
      return false;
    }

    if (valueSelector instanceof NilColumnValueSelector) {
      // Nil column, definitely no SerializablePairLongObject.
      return false;
    }

    // Check if the selector class could possibly be of pairClass* (either a superclass or subclass).
    final Class<?> clazz = valueSelector.classOfObject();
    return clazz.isAssignableFrom(pairClass)
           || pairClass.isAssignableFrom(clazz);
  }

  @Nullable
  public static SerializablePairLongDouble readDoublePairFromVectorSelectors(
      @Nullable boolean[] timeNullityVector,
      long[] timeVector,
      Object[] objectVector,
      int index
  )
  {
    final long time;
    final Double value;

    final Object object = objectVector[index];

    if (object instanceof SerializablePairLongDouble) {
      // We got a folded object, ignore timeSelector completely, the object has all the info it requires
      final SerializablePairLongDouble pair = (SerializablePairLongDouble) object;
      // if time == null, don't aggregate
      if (pair.lhs == null) {
        return null;
      }
      return pair;
    } else {
      if (timeNullityVector != null && timeNullityVector[index]) {
        // Donot aggregate pairs where time is unknown
        return null;
      }
      time = timeVector[index];
      value = DimensionHandlerUtils.convertObjectToDouble(object);
    }
    return new SerializablePairLongDouble(time, value);
  }

  @Nullable
  public static SerializablePairLongFloat readFloatPairFromVectorSelectors(
      @Nullable boolean[] timeNullityVector,
      long[] timeVector,
      Object[] objectVector,
      int index
  )
  {
    final long time;
    final Float value;

    final Object object = objectVector[index];

    if (object instanceof SerializablePairLongFloat) {
      // We got a folded object, ignore timeSelector completely, the object has all the info it requires
      final SerializablePairLongFloat pair = (SerializablePairLongFloat) object;
      // if time == null, don't aggregate
      if (pair.lhs == null) {
        return null;
      }
      return pair;
    } else {
      if (timeNullityVector != null && timeNullityVector[index]) {
        // Donot aggregate pairs where time is unknown
        return null;
      }
      time = timeVector[index];
      value = DimensionHandlerUtils.convertObjectToFloat(object);
    }
    return new SerializablePairLongFloat(time, value);
  }
  @Nullable
  public static SerializablePairLongLong readLongPairFromVectorSelectors(
      @Nullable boolean[] timeNullityVector,
      long[] timeVector,
      Object[] objectVector,
      int index
  )
  {
    final long time;
    final Long value;

    final Object object = objectVector[index];

    if (object instanceof SerializablePairLongLong) {
      // We got a folded object, ignore timeSelector completely, the object has all the info it requires
      final SerializablePairLongLong pair = (SerializablePairLongLong) object;
      // if time == null, don't aggregate
      if (pair.lhs == null) {
        return null;
      }
      return pair;
    } else {
      if (timeNullityVector != null && timeNullityVector[index]) {
        // Donot aggregate pairs where time is unknown
        return null;
      }
      time = timeVector[index];
      value = DimensionHandlerUtils.convertObjectToLong(object);
    }
    return new SerializablePairLongLong(time, value);
  }
}
