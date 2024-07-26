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
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class Transformer
{
  private final Map<String, RowFunction> transforms = new HashMap<>();
  private final ThreadLocal<Row> rowSupplierForValueMatcher = new ThreadLocal<>();
  private final ValueMatcher valueMatcher;

  Transformer(final TransformSpec transformSpec)
  {
    for (final Transform transform : transformSpec.getTransforms()) {
      transforms.put(transform.getName(), transform.getRowFunction());
    }

    if (transformSpec.getFilter() != null) {
      valueMatcher = transformSpec.getFilter().toFilter()
                                  .makeMatcher(
                                      RowBasedColumnSelectorFactory.create(
                                          RowAdapters.standardRow(),
                                          rowSupplierForValueMatcher::get,
                                          RowSignature.empty(), // sad
                                          false,
                                          false
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
      if (!valueMatcher.matches(false)) {
        return null;
      }
    }

    return transformedRow;
  }

  @Nullable
  public InputRowListPlusRawValues transform(@Nullable final InputRowListPlusRawValues row)
  {
    if (row == null) {
      return null;
    }

    final InputRowListPlusRawValues inputRowListPlusRawValues;

    if (transforms.isEmpty() || row.getInputRows() == null) {
      inputRowListPlusRawValues = row;
    } else {
      final List<InputRow> originalRows = row.getInputRows();
      final List<InputRow> transformedRows = new ArrayList<>(originalRows.size());
      for (InputRow originalRow : originalRows) {
        try {
          transformedRows.add(new TransformedInputRow(originalRow, transforms));
        }
        catch (ParseException pe) {
          return InputRowListPlusRawValues.of(row.getRawValues(), pe);
        }
      }
      inputRowListPlusRawValues = InputRowListPlusRawValues.ofList(row.getRawValuesList(), transformedRows);
    }

    if (valueMatcher != null) {
      if (inputRowListPlusRawValues.getInputRows() != null) {
        // size of inputRows and rawValues are the same
        int size = inputRowListPlusRawValues.getInputRows().size();
        final List<InputRow> matchedRows = new ArrayList<>(size);
        final List<Map<String, Object>> matchedVals = new ArrayList<>(size);

        final List<InputRow> inputRows = inputRowListPlusRawValues.getInputRows();
        final List<Map<String, Object>> inputVals = inputRowListPlusRawValues.getRawValuesList();
        for (int i = 0; i < size; i++) {
          rowSupplierForValueMatcher.set(inputRows.get(i));
          if (valueMatcher.matches(false)) {
            matchedRows.add(inputRows.get(i));
            matchedVals.add(inputVals.get(i));
          }
        }
        return InputRowListPlusRawValues.ofList(matchedVals, matchedRows);
      }
    }

    return inputRowListPlusRawValues;
  }
}
