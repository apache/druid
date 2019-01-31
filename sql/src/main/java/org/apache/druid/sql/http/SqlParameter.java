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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import java.util.Objects;

public class SqlParameter
{
  private int ordinal;
  private SqlType type;
  private Object value;

  @JsonCreator
  public SqlParameter(
      @JsonProperty("ordinal") int ordinal,
      @JsonProperty("type") SqlType type,
      @JsonProperty("value") Object value
  )
  {
    this.ordinal = ordinal;
    this.type = type;
    this.value = value;
  }

  @JsonProperty
  public int getOrdinal()
  {
    return ordinal;
  }

  @JsonProperty
  public Object getValue()
  {
    return value;
  }

  @JsonProperty
  public SqlType getType()
  {
    return type;
  }

  @JsonIgnore
  public TypedValue getTypedValue()
  {
    // TypedValue.create for TIMESTAMP expects a long...
    // but be lenient try to accept iso format and sql 'timestamp' format
    if (type == SqlType.TIMESTAMP) {
      if (value instanceof String) {
        try {
          DateTime isIso = DateTimes.of((String) value);
          return TypedValue.create(ColumnMetaData.Rep.nonPrimitiveRepOf(type).name(), isIso.getMillis());
        }
        catch (IllegalArgumentException ignore) {
        }
        try {
          TimestampString isString = new TimestampString((String) value);
          return TypedValue.create(ColumnMetaData.Rep.nonPrimitiveRepOf(type).name(), isString.getMillisSinceEpoch());
        }
        catch (IllegalArgumentException ignore) {
        }
      }
    }
    return TypedValue.create(ColumnMetaData.Rep.nonPrimitiveRepOf(type).name(), value);
  }

  @Override
  public String toString()
  {
    return "SqlParameter{" +
           "ordinal=" + ordinal +
           ", value={" + type.name() + ',' + value + '}' +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SqlParameter that = (SqlParameter) o;
    return ordinal == that.ordinal &&
           Objects.equals(type, that.type) &&
           Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ordinal, type, value);
  }
}
