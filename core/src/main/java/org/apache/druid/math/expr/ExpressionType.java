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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.BaseTypeSignature;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.Types;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;

@JsonSerialize(using = ToStringSerializer.class)
public class ExpressionType extends BaseTypeSignature<ExprType>
{
  public static final ExpressionType STRING = new ExpressionType(ExprType.STRING, null, null);
  public static final ExpressionType LONG = new ExpressionType(ExprType.LONG, null, null);
  public static final ExpressionType DOUBLE = new ExpressionType(ExprType.DOUBLE, null, null);
  public static final ExpressionType STRING_ARRAY = new ExpressionType(ExprType.ARRAY, null, STRING);
  public static final ExpressionType LONG_ARRAY = new ExpressionType(ExprType.ARRAY, null, LONG);
  public static final ExpressionType DOUBLE_ARRAY = new ExpressionType(ExprType.ARRAY, null, DOUBLE);

  @JsonCreator
  public ExpressionType(
      @JsonProperty("type") ExprType exprType,
      @JsonProperty("complexTypeName") @Nullable String complexTypeName,
      @JsonProperty("elementType") @Nullable ExpressionType elementType
  )
  {
    super(exprType, complexTypeName, elementType);
  }

  @Nullable
  @JsonCreator
  public static ExpressionType fromString(@Nullable String typeName)
  {
    return Types.fromString(ExpressionTypeFactory.getInstance(), typeName);
  }

  @Nullable
  public static ExpressionType elementType(@Nullable ExpressionType type)
  {
    if (type != null && type.isArray()) {
      return (ExpressionType) type.getElementType();
    }
    return type;
  }

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
   * this method will convert {@link ValueType#FLOAT} to {@link #DOUBLE}.
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
        throw new ISE("Unsupported array type[%s]", valueType);
      case COMPLEX:
        return new ExpressionType(ExprType.COMPLEX, valueType.getComplexTypeName(), null);
      default:
        throw new ISE("Unsupported value type[%s]", valueType);
    }
  }


  /**
   * The expression system does not distinguish between {@link ValueType#FLOAT} and {@link ValueType#DOUBLE}, and
   * cannot currently handle {@link ValueType#COMPLEX} inputs. This method will convert {@link ValueType#FLOAT} to
   * {@link #DOUBLE}, or null if a null {@link ValueType#COMPLEX} is encountered.
   * @param valueType
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
        return null;
      case COMPLEX:
        return new ExpressionType(ExprType.COMPLEX, valueType.getComplexTypeName(), null);
      default:
        return null;
    }
  }

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
            throw new ISE("Unsupported expression type[%s]", exprType);
        }
      case COMPLEX:
        return new ColumnType(ValueType.COMPLEX, exprType.getComplexTypeName(), null);
      default:
        throw new ISE("Unsupported expression type[%s]", exprType);
    }
  }
}
