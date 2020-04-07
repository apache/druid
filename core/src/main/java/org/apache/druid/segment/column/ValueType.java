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

public enum ValueType
{
  DOUBLE,
  FLOAT,
  LONG,
  STRING,
  COMPLEX,
  DOUBLE_ARRAY,
  LONG_ARRAY,
  STRING_ARRAY;


  public boolean isNumeric()
  {
    return isNumeric(this);
  }

  public boolean isPrimitiveScalar()
  {
    return this.equals(ValueType.STRING) || isNumeric(this);
  }

  public boolean isComplex()
  {
    return !isPrimitiveScalar();
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
