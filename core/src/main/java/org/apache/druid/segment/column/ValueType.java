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

package org.apache.druid.segment.column;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;

/**
 * This enumeration defines the Druid type system used to indicate the type of data stored in columns, produced by
 * expressions, used to allow query processing engine algorithms to compute results, used to compute query result
 * row signatures, and all other type needs.
 *
 * Currently only the primitive types ({@link #isPrimitive()} is true) and {@link #COMPLEX} can be stored in columns
 * and are also the only types handled directly by the query engines. Array types can currently be produced by
 * expressions and by some post-aggregators, but do not currently have special engine handling, and should be used by
 * implementors sparingly until full engine support is in place. Aggregators should never specify array types as their
 * output type until the engines fully support these types.
 */
public enum ValueType
{
  // primitive types
  DOUBLE,
  FLOAT,
  LONG,
  STRING,
  // non-primitive types
  COMPLEX,
  // transient array types (also non-primitive)
  DOUBLE_ARRAY,
  LONG_ARRAY,
  STRING_ARRAY;


  /**
   * Type is a numeric type, not including numeric array types
   */
  public boolean isNumeric()
  {
    return isNumeric(this);
  }

  /**
   * Type is a 'primitive' type, which includes the {@link #isNumeric} types and {@link #STRING}, but not
   * {@link #COMPLEX} or array types.
   */
  public boolean isPrimitive()
  {
    return this.equals(ValueType.STRING) || isNumeric(this);
  }

  @Nullable
  @JsonCreator
  public static ValueType fromString(@Nullable String name)
  {
    if (name == null) {
      return null;
    }
    return valueOf(StringUtils.toUpperCase(name));
  }

  public static boolean isNumeric(ValueType type)
  {
    return type == ValueType.LONG || type == ValueType.FLOAT || type == ValueType.DOUBLE;
  }
}
