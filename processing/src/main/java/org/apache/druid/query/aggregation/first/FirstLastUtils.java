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

package org.apache.druid.query.aggregation.first;

import org.apache.druid.segment.BaseObjectColumnValueSelector;
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

  /**
   * Returns whether an object *might* is assignable to/from the pairClass.
   */
  public static boolean objectNeedsFoldCheck(Object obj, Class pairClass)
  {
    if (obj == null) {
      return false;
    }
    final Class<?> clazz = obj.getClass();
    return clazz.isAssignableFrom(pairClass)
           || pairClass.isAssignableFrom(clazz);
  }
}
