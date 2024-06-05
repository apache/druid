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

import io.delta.kernel.expressions.Literal;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
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

public class DeltaFilterUtils
{
  /**
   * @return a Delta typed literal with the type of value inferred from the snapshot schema. The column must
   * be present in the supplied snapshot schema.
   */
  static Literal dataTypeToLiteral(
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
      } else if (dataType instanceof FloatType) {
        return Literal.ofFloat(Float.parseFloat(value));
      } else if (dataType instanceof DoubleType) {
        return Literal.ofDouble(Double.parseDouble(value));
      } else if (dataType instanceof DateType) {
        final Date dataVal = Date.valueOf(value);
        final int daysSinceEpoch = (int) ChronoUnit.DAYS.between(
            LocalDate.ofEpochDay(0), dataVal.toLocalDate()
        );
        return Literal.ofDate(daysSinceEpoch);
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
}
