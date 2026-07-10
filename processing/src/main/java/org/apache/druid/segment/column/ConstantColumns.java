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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.data.ConstantColumnarInts;
import org.apache.druid.segment.data.GenericIndexed;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Fabricates in-memory {@link BaseColumnHolder}s for columns whose value is constant across every row, without any
 * on-disk data. This is the runtime analogue of {@link org.apache.druid.segment.serde.NullColumnPartSerde} for
 * arbitrary constants: a STRING constant is presented as a single-value {@link StringUtf8DictionaryEncodedColumn}
 * (backed by a one-entry dictionary), and numeric constants as a {@link ConstantNumericColumn}.
 */
public final class ConstantColumns
{
  private ConstantColumns()
  {
    // no instantiation
  }

  /**
   * Add a constant column (via {@link #makeConstantColumnHolder}) to {@code target} for each column in
   * {@code clusteringColumns}, using the group-constant value at the matching position of {@code clusteringValues}.
   */
  public static void addConstantClusteringColumns(
      Map<String, Supplier<BaseColumnHolder>> target,
      RowSignature clusteringColumns,
      Object[] clusteringValues,
      int numRows,
      BitmapFactory bitmapFactory
  )
  {
    DruidException.conditionalDefensive(
        clusteringValues.length == clusteringColumns.size(),
        "clusteringValues length [%s] must match clusteringColumns size [%s]",
        clusteringValues.length,
        clusteringColumns.size()
    );
    for (int i = 0; i < clusteringColumns.size(); i++) {
      final String columnName = clusteringColumns.getColumnName(i);
      final ColumnType columnType = clusteringColumns.getColumnType(i)
                                                     .orElseThrow(() -> DruidException.defensive(
                                                         "clustering column [%s] is missing a type",
                                                         columnName
                                                     ));
      final BaseColumnHolder holder = makeConstantColumnHolder(columnType, clusteringValues[i], numRows, bitmapFactory);
      target.put(columnName, Suppliers.ofInstance(holder));
    }
  }

  public static BaseColumnHolder makeConstantColumnHolder(
      ColumnType type,
      @Nullable Object value,
      int numRows,
      BitmapFactory bitmapFactory
  )
  {
    final ColumnBuilder builder = new ColumnBuilder()
        .setType(type)
        .setHasMultipleValues(false)
        .setHasNulls(value == null);

    if (type.is(ValueType.STRING)) {
      final ByteBuffer utf8 = value == null ? null : StringUtils.toUtf8ByteBuffer((String) value);
      final GenericIndexed<ByteBuffer> dictionary = GenericIndexed.fromIterable(
          Collections.singletonList(utf8),
          GenericIndexed.UTF8_STRATEGY
      );
      builder.setDictionaryEncodedColumnSupplier(
          Suppliers.ofInstance(
              new StringUtf8DictionaryEncodedColumn(
                  new ConstantColumnarInts(numRows, 0),
                  null,
                  dictionary,
                  bitmapFactory
              )
          )
      );
    } else if (type.isNumeric()) {
      builder.setNumericColumnSupplier(
          Suppliers.ofInstance(new ConstantNumericColumn(type, (Number) value, numRows))
      );
    } else {
      throw DruidException.defensive("Cannot build a constant column for type[%s]", type);
    }
    return builder.build();
  }
}
