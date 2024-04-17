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

package org.apache.druid.delta.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.InvalidInput;

import java.sql.Date;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;

public abstract class DeltaBinaryOperatorFilter implements DeltaFilter
{
  @JsonProperty
  private final String operator;
  @JsonProperty
  private final String column;
  @JsonProperty
  private final String value;

  private DeltaBinaryOperatorFilter(
      final String operator,
      final String column,
      final String value
  )
  {
    if (operator == null) {
      throw InvalidInput.exception("operator is required for Delta filters.");
    }
    if (column == null) {
      throw InvalidInput.exception("column is required for Delta filters.");
    }
    if (value == null) {
      throw InvalidInput.exception("value is required for Delta filters.");
    }
    this.operator = operator;
    this.column = column;
    this.value = value;
  }

  @Override
  public Predicate getFilterPredicate(StructType snapshotSchema)
  {
    return new Predicate(
        operator,
        ImmutableList.of(
            new Column(column),
            dataTypeToLiteral(snapshotSchema, column, value)
        )
    );
  }

  private static Literal dataTypeToLiteral(
      final StructType snapshotSchema,
      final String column,
      final String value
  )
  {
    if (!snapshotSchema.fieldNames().contains(column)) {
      throw InvalidInput.exception(
          "column[%s] doesn't exist in schema[%s]", column, snapshotSchema
      );
    }

    final StructField structField = snapshotSchema.get(column);
    final DataType dataType = structField.getDataType();
    try {
      if (dataType instanceof StringType) {
        return Literal.ofString(value);
      } else if (dataType instanceof IntegerType) {
        return Literal.ofInt(Integer.parseInt(value));
      } else if (dataType instanceof ShortType) {
        return Literal.ofShort(Short.parseShort(value));
      } else if (dataType instanceof LongType) {
        return Literal.ofLong(Long.parseLong(value));
      } else if (dataType instanceof DoubleType) {
        return Literal.ofDouble(Double.parseDouble(value));
      } else if (dataType instanceof DateType) {
        final Date dataVal = Date.valueOf(value);
        final LocalDate localDate = dataVal.toLocalDate();
        final LocalDate epoch = LocalDate.ofEpochDay(0);
        int between = (int) ChronoUnit.DAYS.between(epoch, localDate);
        return Literal.ofDate(between);
      } else {
        throw InvalidInput.exception(
            "Unsupported data type[%s] for column[%s] with value[%s].",
            dataType, column, value
        );
      }
    }
    catch (NumberFormatException e) {
      throw InvalidInput.exception(
          "column[%s] has an invalid value[%s]. The value must be a number, as the column's data type is [%s].",
          column, value, dataType
      );
    }
  }

  public static class DeltaEqualsFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaEqualsFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super("=", column, value);
    }
  }


  public static class DeltaGreaterThanFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaGreaterThanFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super(">", column, value);
    }
  }

  public static class DeltaGreaterThanOrEqualsFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaGreaterThanOrEqualsFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super(">=", column, value);
    }
  }


  public static class DeltaLessThanFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaLessThanFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super("<", column, value);
    }
  }

  public static class DeltaLessThanOrEqualsFilter extends DeltaBinaryOperatorFilter
  {
    @JsonCreator
    public DeltaLessThanOrEqualsFilter(@JsonProperty("column") final String column, @JsonProperty("value") final String value)
    {
      super("<=", column, value);
    }
  }
}
