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

package org.apache.druid.math.expr;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * Base 'value' types of Druid expression language, all {@link Expr} must evaluate to one of these types.
 */
public enum ExprType
{
  DOUBLE,
  LONG,
  STRING,
  DOUBLE_ARRAY,
  LONG_ARRAY,
  STRING_ARRAY;

  /**
   * The expression system does not distinguish between {@link ValueType#FLOAT} and {@link ValueType#DOUBLE}, and
   * cannot currently handle {@link ValueType#COMPLEX} inputs. This method will convert {@link ValueType#FLOAT} to
   * {@link #DOUBLE}, or throw an exception if a {@link ValueType#COMPLEX} is encountered.
   *
   * @throws IllegalStateException
   */
  public static ExprType fromValueType(@Nullable ValueType valueType)
  {
    if (valueType == null) {
      throw new IllegalStateException("Unsupported unknown value type");
    }
    switch (valueType) {
      case LONG:
        return LONG;
      case LONG_ARRAY:
        return LONG_ARRAY;
      case FLOAT:
      case DOUBLE:
        return DOUBLE;
      case DOUBLE_ARRAY:
        return DOUBLE_ARRAY;
      case STRING:
        return STRING;
      case STRING_ARRAY:
        return STRING_ARRAY;
      case COMPLEX:
      default:
        throw new ISE("Unsupported value type[%s]", valueType);
    }
  }
}
