/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.indexing;

import com.google.common.base.Strings;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.ISE;
import io.druid.math.expr.Expr;
import io.druid.math.expr.ExprEval;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class Transformer
{
  private final Map<String, Expr> transforms = new HashMap<>();
  private final ThreadLocal<Row> rowSupplierForValueMatcher = new ThreadLocal<>();
  private final ValueMatcher valueMatcher;

  Transformer(final TransformSpec transformSpec)
  {
    for (final Transform transform : transformSpec.getTransforms()) {
      transforms.put(transform.getName(), transform.toExpr());
    }

    if (transformSpec.getFilter() != null) {
      valueMatcher = transformSpec.getFilter().toFilter()
                                  .makeMatcher(
                                      RowBasedColumnSelectorFactory.create(
                                          rowSupplierForValueMatcher,
                                          null
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
    private final Map<String, Expr> transforms;

    public TransformedInputRow(final InputRow row, final Map<String, Expr> transforms)
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
      final Expr transform = transforms.get(Column.TIME_COLUMN_NAME);
      if (transform != null) {
        return transform.eval(this::getValueFromRow).asLong();
      } else {
        return row.getTimestampFromEpoch();
      }
    }

    @Override
    public DateTime getTimestamp()
    {
      final Expr transform = transforms.get(Column.TIME_COLUMN_NAME);
      if (transform != null) {
        return DateTimes.utc(transform.eval(this::getValueFromRow).asLong());
      } else {
        return row.getTimestamp();
      }
    }

    @Override
    public List<String> getDimension(final String dimension)
    {
      final Expr transform = transforms.get(dimension);
      if (transform != null) {
        // Always return single-value. Expressions don't support array/list operations yet.
        final String s = transform.eval(this::getValueFromRow).asString();
        if (Strings.isNullOrEmpty(s)) {
          return Collections.emptyList();
        } else {
          return Collections.singletonList(s);
        }
      } else {
        return row.getDimension(dimension);
      }
    }

    @Override
    public Object getRaw(final String column)
    {
      final Expr transform = transforms.get(column);
      if (transform != null) {
        return transform.eval(this::getValueFromRow).value();
      } else {
        return row.getRaw(column);
      }
    }

    @Override
    public Number getMetric(final String metric)
    {
      final Expr transform = transforms.get(metric);
      if (transform != null) {
        final ExprEval eval = transform.eval(this::getValueFromRow);
        switch (eval.type()) {
          case DOUBLE:
            return eval.asDouble();
          case LONG:
            return eval.asLong();
          case STRING:
            return Rows.stringToNumber(metric, eval.asString());
          default:
            throw new ISE("WTF, unexpected eval type[%s]", eval.type());
        }
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

    private Object getValueFromRow(final String column)
    {
      if (column.equals(Column.TIME_COLUMN_NAME)) {
        return row.getTimestampFromEpoch();
      } else {
        return row.getRaw(column);
      }
    }
  }
}
