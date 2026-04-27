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
  private final List<Transform> multiRowTransforms = new ArrayList<>();
  private final ThreadLocal<Row> rowSupplierForValueMatcher = new ThreadLocal<>();
  private final ValueMatcher valueMatcher;

  Transformer(final TransformSpec transformSpec)
  {
    for (final Transform transform : transformSpec.getTransforms()) {
      if (transform.isMultiRow()) {
        multiRowTransforms.add(transform);
      } else {
        transforms.put(transform.getName(), transform.getRowFunction());
      }
    }

    if (transformSpec.getFilter() != null) {
      valueMatcher = transformSpec.getFilter().toFilter()
                                  .makeMatcher(
                                      RowBasedColumnSelectorFactory.create(
                                          RowAdapters.standardRow(),
                                          rowSupplierForValueMatcher::get,
                                          RowSignature.empty(), // sad
                                          false
                                      )
                                  );
    } else {
      valueMatcher = null;
    }
  }

  /**
   * Whether any multi-row transforms are configured.
   */
  public boolean hasMultiRowTransform()
  {
    return !multiRowTransforms.isEmpty();
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

  /**
   * Transforms an input row, returning zero or more output rows.
   * Applies single-row transforms and filtering first, then chains multi-row transforms sequentially.
   */
  public List<InputRow> transformToList(@Nullable final InputRow row)
  {
    final InputRow singleRowResult = transform(row);
    if (singleRowResult == null) {
      return List.of();
    }

    return applyMultiRowTransforms(singleRowResult);
  }

  private List<InputRow> applyMultiRowTransforms(final InputRow inputRow)
  {
    if (multiRowTransforms.isEmpty()) {
      return List.of(inputRow);
    }

    List<InputRow> current = List.of(inputRow);
    for (final Transform multiRowTransform : multiRowTransforms) {
      final List<InputRow> next = new ArrayList<>();
      for (final InputRow currentRow : current) {
        next.addAll(multiRowTransform.applyMultiRow(currentRow));
      }
      current = next;
    }

    return current;
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

    return applyMultiRowTransforms(inputRowListPlusRawValues);
  }

  private InputRowListPlusRawValues applyMultiRowTransforms(final InputRowListPlusRawValues row)
  {
    if (multiRowTransforms.isEmpty() || row.getInputRows() == null) {
      return row;
    }

    final List<InputRow> inputRows = row.getInputRows();
    final List<Map<String, Object>> inputRawValues = row.getRawValuesList();
    final List<InputRow> outputRows = new ArrayList<>();
    final List<Map<String, Object>> outputRawValues = inputRawValues == null ? null : new ArrayList<>();

    for (int i = 0; i < inputRows.size(); i++) {
      final List<InputRow> expandedRows = applyMultiRowTransforms(inputRows.get(i));
      outputRows.addAll(expandedRows);
      if (outputRawValues != null) {
        for (int j = 0; j < expandedRows.size(); j++) {
          outputRawValues.add(inputRawValues.get(i));
        }
      }
    }

    return InputRowListPlusRawValues.ofList(outputRawValues, outputRows, row.getParseException());
  }
}
