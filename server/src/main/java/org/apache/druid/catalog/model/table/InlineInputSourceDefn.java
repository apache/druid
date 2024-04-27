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

package org.apache.druid.catalog.model.table;

import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.BaseTableFunction.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterType;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Describes an inline input source: one where the data is provided in the
 * table spec as a series of text lines. Since the data is provided, the input
 * format is required in the table spec: it cannot be provided at ingest time.
 * Primarily for testing.
 */
public class InlineInputSourceDefn extends FormattedInputSourceDefn
{
  public static final String TYPE_KEY = InlineInputSource.TYPE_KEY;
  public static final String DATA_PROPERTY = "data";
  public static final String DATA_FIELD = "data";

  @Override
  public String typeValue()
  {
    return TYPE_KEY;
  }

  @Override
  protected Class<? extends InputSource> inputSourceClass()
  {
    return InlineInputSource.class;
  }

  @Override
  protected List<ParameterDefn> adHocTableFnParameters()
  {
    return Collections.singletonList(
        new Parameter(DATA_PROPERTY, ParameterType.VARCHAR_ARRAY, false)
    );
  }

  @Override
  public TableFunction partialTableFn(ResolvedExternalTable table)
  {
    return new PartialTableFunction(table, Collections.emptyList());
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    // For inline, format is required to match the data
    if (table.inputFormatMap == null) {
      throw new IAE("An inline input source must provide a format.");
    }
    if (CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns())) {
      throw new IAE("An inline input source must provide one or more columns");
    }

    super.validate(table);
  }

  @Override
  protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    jsonMap.put(InputSource.TYPE_PROPERTY, InlineInputSource.TYPE_KEY);
    List<String> data = CatalogUtils.getStringArray(args, DATA_PROPERTY);

    // Would be nice, from a completeness perspective, for the inline data
    // source to allow zero rows of data. However, such is not the case.
    if (CollectionUtils.isNullOrEmpty(data)) {
      throw new IAE(
          "An inline table requires one or more rows of data in the '%s' property",
          DATA_PROPERTY
      );
    }
    jsonMap.put(DATA_FIELD, String.join("\n", data));
  }

  @Override
  protected ExternalTableSpec convertCompletedTable(
      final ResolvedExternalTable table,
      final Map<String, Object> args,
      final List<ColumnSpec> columns
  )
  {
    if (!args.isEmpty()) {
      throw new ISE("Cannot provide arguments for an inline table");
    }
    if (!CollectionUtils.isNullOrEmpty(columns)) {
      throw new IAE("Cannot provide columns for an inline table");
    }
    return convertTable(table);
  }

  @Override
  protected void auditInputSource(Map<String, Object> jsonMap)
  {
    // Special handling of the data property which, in SQL, is a null-delimited
    // list of rows. The user will usually provide a trailing newline which should
    // not be interpreted as an empty data row. That is, if the data ends with
    // a newline, the inline input source will interpret that as a blank line, oddly.
    String data = CatalogUtils.getString(jsonMap, DATA_FIELD);
    if (data != null && !data.endsWith("\n")) {
      jsonMap.put(DATA_FIELD, data + "\n");
    }
  }
}
