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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.BaseTypeSignature;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

/**
 * The type system used to process Druid expressions. This is basically {@link ColumnType}, but without
 * {@link ColumnType#FLOAT} because the expression processing system does not currently directly support them.
 */
@JsonSerialize(using = ToStringSerializer.class)
public class ExpressionType extends BaseTypeSignature<ExprType>
{
  public static final ExpressionType STRING =
      new ExpressionType(ExprType.STRING, null, null);
  public static final ExpressionType LONG =
      new ExpressionType(ExprType.LONG, null, null);
  public static final ExpressionType DOUBLE =
      new ExpressionType(ExprType.DOUBLE, null, null);
  public static final ExpressionType STRING_ARRAY =
      new ExpressionType(ExprType.ARRAY, null, STRING);
  public static final ExpressionType LONG_ARRAY =
      new ExpressionType(ExprType.ARRAY, null, LONG);
  public static final ExpressionType DOUBLE_ARRAY =
      new ExpressionType(ExprType.ARRAY, null, DOUBLE);
  public static final ExpressionType UNKNOWN_COMPLEX =
      new ExpressionType(ExprType.COMPLEX, null, null);

  @JsonCreator
  public ExpressionType(
      @JsonProperty("type") ExprType exprType,
      @JsonProperty("complexTypeName") @Nullable String complexTypeName,
      @JsonProperty("elementType") @Nullable ExpressionType elementType
  )
  {
    super(ExpressionTypeFactory.getInstance(), exprType, complexTypeName, elementType);
  }

  @Nullable
  @JsonCreator
  public static ExpressionType fromString(@Nullable String typeName)
  {
    return Types.fromString(ExpressionTypeFactory.getInstance(), typeName);
  }

  /**
   * If an {@link ExpressionType} is an array, return {@link ExpressionType#getElementType()}, otherwise the type is
   * returned unchanged.
   */
  @Nullable
  public static ExpressionType elementType(@Nullable ExpressionType type)
  {
    if (type != null && type.isArray()) {
      return (ExpressionType) type.getElementType();
    }
    return type;
  }

  /**
   * Convert a primitive {@link ExpressionType} into an array of that type. Non-primitive types are passed through,
   * even if they are not arrays.
   */
  @Nullable
  public static ExpressionType asArrayType(@Nullable ExpressionType elementType)
  {
    if (elementType != null && elementType.isPrimitive()) {
      switch (elementType.getType()) {
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


  /**
   * The expression system does not distinguish between {@link ValueType#FLOAT} and {@link ValueType#DOUBLE}, so,
   * this method will convert {@link ValueType#FLOAT} to {@link #DOUBLE}. Null values are not allowed in this method,
   * and will result in an {@link IllegalStateException}
   *
   * @throws IllegalStateException
   */
  public static ExpressionType fromColumnTypeStrict(@Nullable TypeSignature<ValueType> valueType)
  {
    if (valueType == null) {
      throw new IllegalStateException("Unsupported unknown value type");
    }
    switch (valueType.getType()) {
      case LONG:
        return LONG;
      case FLOAT:
      case DOUBLE:
        return DOUBLE;
      case STRING:
        return STRING;
      case ARRAY:
        switch (valueType.getElementType().getType()) {
          case LONG:
            return LONG_ARRAY;
          case DOUBLE:
            return DOUBLE_ARRAY;
          case STRING:
            return STRING_ARRAY;
        }
        return ExpressionTypeFactory.getInstance().ofArray(fromColumnTypeStrict(valueType.getElementType()));
      case COMPLEX:
        return ExpressionTypeFactory.getInstance().ofComplex(valueType.getComplexTypeName());
      default:
        throw new ISE("Unsupported value type[%s]", valueType);
    }
  }


  /**
   * The expression system does not distinguish between {@link ValueType#FLOAT} and {@link ValueType#DOUBLE}, so this
   * method will convert {@link ValueType#FLOAT} to {@link #DOUBLE}.
   */
  @Nullable
  public static ExpressionType fromColumnType(@Nullable TypeSignature<ValueType> valueType)
  {
    if (valueType == null) {
      return null;
    }
    switch (valueType.getType()) {
      case LONG:
        return LONG;
      case FLOAT:
      case DOUBLE:
        return DOUBLE;
      case STRING:
        return STRING;
      case ARRAY:
        switch (valueType.getElementType().getType()) {
          case LONG:
            return LONG_ARRAY;
          case DOUBLE:
            return DOUBLE_ARRAY;
          case STRING:
            return STRING_ARRAY;
        }
        return ExpressionTypeFactory.getInstance().ofArray(fromColumnType(valueType.getElementType()));
      case COMPLEX:
        return ExpressionTypeFactory.getInstance().ofComplex(valueType.getComplexTypeName());
      default:
        return null;
    }
  }

  /**
   * Convert {@link ExpressionType} to the corresponding {@link ColumnType}
   */
  public static ColumnType toColumnType(ExpressionType exprType)
  {
    switch (exprType.getType()) {
      case LONG:
        return ColumnType.LONG;
      case DOUBLE:
        return ColumnType.DOUBLE;
      case STRING:
        return ColumnType.STRING;
      case ARRAY:
        switch (exprType.getElementType().getType()) {
          case LONG:
            return ColumnType.LONG_ARRAY;
          case DOUBLE:
            return ColumnType.DOUBLE_ARRAY;
          case STRING:
            return ColumnType.STRING_ARRAY;
          default:
            return ColumnType.ofArray(toColumnType((ExpressionType) exprType.getElementType()));
        }
      case COMPLEX:
        return ColumnType.ofComplex(exprType.getComplexTypeName());
      default:
        throw new ISE("Unsupported expression type[%s]", exprType);
    }
  }

  public static void checkNestedArrayAllowed(ExpressionType outputType)
  {
    if (outputType.isArray() && outputType.getElementType().isArray() && !ExpressionProcessing.allowNestedArrays()) {
      throw new IAE("Cannot create a nested array type [%s], 'druid.expressions.allowNestedArrays' must be set to true", outputType);
    }
  }
}
