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
import com.google.common.base.Preconditions;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.avatica.remote.TypedValue;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.util.TimestampString;
import org.apache.druid.java.util.common.DateTimes;

import javax.annotation.Nullable;
import java.sql.Date;
import java.util.Objects;

public class SqlParameter
{
  private final SqlType type;
  private final Object value;

  @JsonCreator
  public SqlParameter(
      @JsonProperty("type") SqlType type,
      @JsonProperty("value") @Nullable Object value
  )
  {
    this.type = Preconditions.checkNotNull(type);
    this.value = value;
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
    Object adjustedValue = value;

    // perhaps there is a better way to do this?
    if (type == SqlType.TIMESTAMP) {
      // TypedValue.create for TIMESTAMP expects a long...
      // but be lenient try to accept iso format and sql 'timestamp' format\
      if (value instanceof String) {
        try {
          adjustedValue = DateTimes.of((String) value).getMillis();
        }
        catch (IllegalArgumentException ignore) {
        }
        try {
          adjustedValue = new TimestampString((String) value).getMillisSinceEpoch();
        }
        catch (IllegalArgumentException ignore) {
        }
      }
    } else if (type == SqlType.DATE) {
      // TypedValue.create for DATE expects calcites internal int representation of sql dates
      // but be lenient try to accept sql date 'yyyy-MM-dd' format and convert to internal calcite int representation
      if (value instanceof String) {
        try {
          adjustedValue = SqlFunctions.toInt(Date.valueOf((String) value));
        }
        catch (IllegalArgumentException ignore) {
        }
      }
    }
    return TypedValue.create(ColumnMetaData.Rep.nonPrimitiveRepOf(type).name(), adjustedValue);
  }

  @Override
  public String toString()
  {
    return "SqlParameter{" +
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
    return Objects.equals(type, that.type) &&
           Objects.equals(value, that.value);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(type, value);
  }
}
