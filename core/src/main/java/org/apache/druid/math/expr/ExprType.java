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


  public boolean isNumeric()
  {
    return isNumeric(this);
  }

  /**
   * The expression system does not distinguish between {@link ValueType#FLOAT} and {@link ValueType#DOUBLE}, and
   * cannot currently handle {@link ValueType#COMPLEX} inputs. This method will convert {@link ValueType#FLOAT} to
   * {@link #DOUBLE}, or throw an exception if a {@link ValueType#COMPLEX} is encountered.
   *
   * @throws IllegalStateException
   */
  public static ExprType fromValueTypeStrict(@Nullable ValueType valueType)
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

  /**
   * The expression system does not distinguish between {@link ValueType#FLOAT} and {@link ValueType#DOUBLE}, and
   * cannot currently handle {@link ValueType#COMPLEX} inputs. This method will convert {@link ValueType#FLOAT} to
   * {@link #DOUBLE}, or null if a null {@link ValueType#COMPLEX} is encountered.
   */
  @Nullable
  public static ExprType fromValueType(@Nullable ValueType valueType)
  {
    if (valueType == null) {
      return null;
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
        return null;
    }
  }


  public static ValueType toValueType(ExprType exprType)
  {
    switch (exprType) {
      case LONG:
        return ValueType.LONG;
      case LONG_ARRAY:
        return ValueType.LONG_ARRAY;
      case DOUBLE:
        return ValueType.DOUBLE;
      case DOUBLE_ARRAY:
        return ValueType.DOUBLE_ARRAY;
      case STRING:
        return ValueType.STRING;
      case STRING_ARRAY:
        return ValueType.STRING_ARRAY;
      default:
        throw new ISE("Unsupported expression type[%s]", exprType);
    }
  }

  public static boolean isNumeric(@Nullable ExprType type)
  {
    return LONG.equals(type) || DOUBLE.equals(type);
  }

  public static boolean isArray(@Nullable ExprType type)
  {
    return LONG_ARRAY.equals(type) || DOUBLE_ARRAY.equals(type) || STRING_ARRAY.equals(type);
  }

  @Nullable
  public static ExprType elementType(@Nullable ExprType type)
  {
    if (type != null) {
      switch (type) {
        case STRING_ARRAY:
          return STRING;
        case LONG_ARRAY:
          return LONG;
        case DOUBLE_ARRAY:
          return DOUBLE;
      }
    }
    return type;
  }

  @Nullable
  public static ExprType asArrayType(@Nullable ExprType elementType)
  {
    if (elementType != null) {
      switch (elementType) {
        case STRING:
          return STRING_ARRAY;
        case LONG:
          return LONG_ARRAY;
        case DOUBLE:
          return DOUBLE_ARRAY;
      }
    }
    return elementType;
  }

}
