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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;

import javax.annotation.Nullable;

@JsonSerialize(using = ToStringSerializer.class)
public class ColumnType extends BaseTypeSignature<ValueType>
{
  public static final ColumnType STRING = new ColumnType(ValueType.STRING, null, null);
  public static final ColumnType LONG = new ColumnType(ValueType.LONG, null, null);
  public static final ColumnType DOUBLE = new ColumnType(ValueType.DOUBLE, null, null);
  public static final ColumnType FLOAT = new ColumnType(ValueType.FLOAT, null, null);
  // currently, arrays only come from expressions or aggregators
  // and there are no native float expressions (or aggs which produce float arrays)
  public static final ColumnType STRING_ARRAY = new ColumnType(ValueType.ARRAY, null, STRING);
  public static final ColumnType LONG_ARRAY = new ColumnType(ValueType.ARRAY, null, LONG);
  public static final ColumnType DOUBLE_ARRAY = new ColumnType(ValueType.ARRAY, null, DOUBLE);
  public static final ColumnType UNKNOWN_COMPLEX = new ColumnType(ValueType.COMPLEX, null, null);

  @JsonCreator
  public ColumnType(
      @JsonProperty("type") ValueType type,
      @JsonProperty("complexTypeName") @Nullable String complexTypeName,
      @JsonProperty("elementType") @Nullable ColumnType elementType
  )
  {
    super(ColumnTypeFactory.getInstance(), type, complexTypeName, elementType);
  }

  @Nullable
  @JsonCreator
  public static ColumnType fromString(@Nullable String typeName)
  {
    return Types.fromString(ColumnTypeFactory.getInstance(), typeName);
  }

  public static ColumnType ofArray(ColumnType elementType)
  {
    return ColumnTypeFactory.getInstance().ofArray(elementType);
  }
  public static ColumnType ofComplex(@Nullable String complexTypeName)
  {
    return ColumnTypeFactory.getInstance().ofComplex(complexTypeName);
  }
}
