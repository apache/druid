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

package org.apache.druid.grpc.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.SqlRowTransformer;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.table.RowSignatures;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Optional;
import java.util.TimeZone;

/**
 * Transforms query result for protobuf format
 */
public class ProtobufTransformer
{

  /**
   * Transform a sql query result into protobuf result format.
   * For complex or missing column type the object is converted into ByteString.
   * date and time column types is converted into proto timestamp.
   * Remaining column types are not converted.
   *
   * @param rowTransformer row signature for sql query result
   * @param row result row
   * @param i index in the result row
   * @return transformed query result in protobuf result format
   */
  @Nullable
  public static Object transform(SqlRowTransformer rowTransformer, Object[] row, int i)
  {
    if (row[i] == null) {
      return null;
    }
    final RelDataType rowType = rowTransformer.getRowType();
    final SqlTypeName sqlTypeName = rowType.getFieldList().get(i).getType().getSqlTypeName();
    final RowSignature signature = RowSignatures.fromRelDataType(rowType.getFieldNames(), rowType);
    final Optional<ColumnType> columnType = signature.getColumnType(i);

    if (sqlTypeName == SqlTypeName.TIMESTAMP
        || sqlTypeName == SqlTypeName.DATE) {
      if (sqlTypeName == SqlTypeName.TIMESTAMP) {
        return convertEpochToProtoTimestamp((long) row[i]);
      }
      return convertDateToProtoTimestamp((int) row[i]);
    }

    if (!columnType.isPresent()) {
      return convertComplexType(row[i]);
    }

    final ColumnType druidType = columnType.get();

    if (druidType == ColumnType.STRING) {
      return row[i];
    } else if (druidType == ColumnType.LONG) {
      return row[i];
    } else if (druidType == ColumnType.FLOAT) {
      return row[i];
    } else if (druidType == ColumnType.DOUBLE) {
      return row[i];
    } else {
      return convertComplexType(row[i]);
    }
  }

  /**
   * Transform a native query result into protobuf result format.
   * For complex or missing column type the object is converted into ByteString.
   * date and time column types are converted into proto timestamp.
   * Remaining column types are not converted.
   *
   * @param rowSignature type signature for a query result row
   * @param row result row
   * @param i index in the result
   * @param convertToTimestamp if the result should be converted to proto timestamp
   * @return transformed query result in protobuf result format
   */
  @Nullable
  public static Object transform(RowSignature rowSignature, Object[] row, int i, boolean convertToTimestamp)
  {
    if (row[i] == null) {
      return null;
    }

    final Optional<ColumnType> columnType = rowSignature.getColumnType(i);

    if (convertToTimestamp) {
      return convertEpochToProtoTimestamp((long) row[i]);
    }

    if (!columnType.isPresent()) {
      return convertComplexType(row[i]);
    }

    final ColumnType druidType = columnType.get();

    if (druidType == ColumnType.STRING) {
      return row[i];
    } else if (druidType == ColumnType.LONG) {
      return row[i];
    } else if (druidType == ColumnType.FLOAT) {
      return row[i];
    } else if (druidType == ColumnType.DOUBLE) {
      return row[i];
    } else {
      return convertComplexType(row[i]);
    }
  }

  public static Timestamp convertEpochToProtoTimestamp(long value)
  {
    DateTime dateTime = Calcites.calciteTimestampToJoda(value, DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));
    long seconds = DateTimeUtils.getInstantMillis(dateTime) / 1000;
    return Timestamp.newBuilder().setSeconds(seconds).build();
  }

  public static Timestamp convertDateToProtoTimestamp(int value)
  {
    DateTime dateTime = Calcites.calciteDateToJoda(value, DateTimeZone.forTimeZone(TimeZone.getTimeZone("UTC")));
    long seconds = DateTimeUtils.getInstantMillis(dateTime) / 1000;
    return Timestamp.newBuilder().setSeconds(seconds).build();
  }

  private static ByteString convertComplexType(Object value)
  {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(bos)) {
      oos.writeObject(value);
      oos.flush();
      return ByteString.copyFrom(bos.toByteArray());
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
