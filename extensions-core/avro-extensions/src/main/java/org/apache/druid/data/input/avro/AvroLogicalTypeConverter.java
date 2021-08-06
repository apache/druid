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

package org.apache.druid.data.input.avro;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;

import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.function.Function;

public class AvroLogicalTypeConverter
{
  private static final Map<Class<? extends LogicalType>, Converter<?>> CONVERSION_MAP =
      ImmutableMap.<Class<? extends LogicalType>, Converter<?>>builder()
          .put(LogicalTypes.Decimal.class, new Converter<>(
              new Conversions.DecimalConversion(),
              BigDecimal::doubleValue
          ))
          .put(LogicalTypes.Date.class, new Converter<>(
              new TimeConversions.DateConversion(),
              d -> d.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
          ))
          .build();

  public Object convert(Object field, Schema schema)
  {
    LogicalType logicalType = schema.getLogicalType();
    Converter<?> converter = CONVERSION_MAP.get(logicalType.getClass());
    return converter.convert(field, schema, logicalType);
  }

  public boolean isRegistered(LogicalType logicalType)
  {
    return CONVERSION_MAP.containsKey(logicalType.getClass());
  }

  private static class Converter<T>
  {
    private final Conversion<T> avroConversion;
    private final Function<T, Object> druidConversion;

    Converter(
        Conversion<T> avroConversion,
        Function<T, Object> druidConversion
    )
    {
      this.avroConversion = avroConversion;
      this.druidConversion = druidConversion;
    }

    public Object convert(Object field, Schema schema, LogicalType logicalType)
    {

      T converted = avroConversion.getConvertedType().isInstance(field)
                    ? (T) field
                    : (T) Conversions.convertToLogicalType(
                        field,
                        schema,
                        logicalType,
                        avroConversion
                    );

      return druidConversion.apply(converted);
    }
  }

}
