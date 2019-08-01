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

package org.apache.druid.segment.transform;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class Transformer
{
  private final Map<String, RowFunction> transforms = new HashMap<>();
  private final ThreadLocal<Row> rowSupplierForValueMatcher = new ThreadLocal<>();
  private final ValueMatcher valueMatcher;

  Transformer(final TransformSpec transformSpec, final Map<String, ValueType> rowSignature)
  {
    for (final Transform transform : transformSpec.getTransforms()) {
      transforms.put(transform.getName(), transform.getRowFunction());
    }

    if (transformSpec.getFilter() != null) {
      valueMatcher = transformSpec.getFilter().toFilter()
                                  .makeMatcher(
                                      RowBasedColumnSelectorFactory.create(
                                          rowSupplierForValueMatcher::get,
                                          rowSignature
                                      )
                                  );
    } else {
      valueMatcher = null;
    }
  }

  /**
   * Transforms an input row, or returns null if the row should be filtered out.
   *
   * @param row the input row
   */
  @Nullable
  public InputRow transform(@Nullable final InputRow row)
  {
    if (row == null) {
      return null;
    }

    final InputRow transformedRow;

    if (transforms.isEmpty()) {
      transformedRow = row;
    } else {
      transformedRow = new TransformedInputRow(row, transforms);
    }

    if (valueMatcher != null) {
      rowSupplierForValueMatcher.set(transformedRow);
      if (!valueMatcher.matches()) {
        return null;
      }
    }

    return transformedRow;
  }

  public static class TransformedInputRow implements InputRow
  {
    private final InputRow row;
    private final Map<String, RowFunction> transforms;

    public TransformedInputRow(final InputRow row, final Map<String, RowFunction> transforms)
    {
      this.row = row;
      this.transforms = transforms;
    }

    @Override
    public List<String> getDimensions()
    {
      return row.getDimensions();
    }

    @Override
    public long getTimestampFromEpoch()
    {
      final RowFunction transform = transforms.get(ColumnHolder.TIME_COLUMN_NAME);
      if (transform != null) {
        return Rows.objectToNumber(ColumnHolder.TIME_COLUMN_NAME, transform.eval(row)).longValue();
      } else {
        return row.getTimestampFromEpoch();
      }
    }

    @Override
    public DateTime getTimestamp()
    {
      final RowFunction transform = transforms.get(ColumnHolder.TIME_COLUMN_NAME);
      if (transform != null) {
        return DateTimes.utc(getTimestampFromEpoch());
      } else {
        return row.getTimestamp();
      }
    }

    @Override
    public List<String> getDimension(final String dimension)
    {
      final RowFunction transform = transforms.get(dimension);
      if (transform != null) {
        return Rows.objectToStrings(transform.eval(row));
      } else {
        return row.getDimension(dimension);
      }
    }

    @Override
    public Object getRaw(final String column)
    {
      final RowFunction transform = transforms.get(column);
      if (transform != null) {
        return transform.eval(row);
      } else {
        return row.getRaw(column);
      }
    }

    @Override
    public Number getMetric(final String metric)
    {
      final RowFunction transform = transforms.get(metric);
      if (transform != null) {
        return Rows.objectToNumber(metric, transform.eval(row));
      } else {
        return row.getMetric(metric);
      }
    }

    @Override
    public boolean equals(final Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final TransformedInputRow that = (TransformedInputRow) o;
      return Objects.equals(row, that.row) &&
             Objects.equals(transforms, that.transforms);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(row, transforms);
    }

    @Override
    public int compareTo(final Row o)
    {
      return row.compareTo(o);
    }
  }
}
