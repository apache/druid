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

package io.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.metamx.common.IAE;

/**
*/
public enum ValueType
{
  FLOAT,
  LONG,
  STRING,
  COMPLEX;

  @JsonCreator
  public static ValueType fromString(String name)
  {
    if (name == null) {
      return ValueType.STRING;
    }
    return valueOf(name.toUpperCase());
  }

  public static ValueType typeFor(Class clazz)
  {
    if (clazz == String.class) {
      return STRING;
    } else if (clazz == float.class || clazz == Float.TYPE) {
      return FLOAT;
    } else if (clazz == long.class || clazz == Long.TYPE) {
      return LONG;
    }
    return COMPLEX;
  }

  public static ValueType convertObjectToValueType(Object valObj)
  {
    if (valObj == null) {
      return null;
    } else if (valObj instanceof ValueType) {
      return (ValueType) valObj;
    } else if (valObj instanceof String) {
      return fromString((String) valObj);
    } else {
      throw new IAE("Cannot convert [%s] to ValueType", valObj);
    }
  }
}
