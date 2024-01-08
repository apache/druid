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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.BaseTableFunction.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterType;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for input formats that require an input format (which is most of them.)
 * By default, an input source supports all formats defined in the table registry, but
 * specific input sources can be more restrictive. The list of formats defines the list
 * of SQL function arguments available when defining a table from scratch.
 */
public abstract class FormattedInputSourceDefn extends BaseInputSourceDefn
{
  public static final String FORMAT_PARAMETER = "format";

  private Map<String, InputFormatDefn> formats;

  @Override
  public void bind(TableDefnRegistry registry)
  {
    formats = registry.formats();
    super.bind(registry);
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    final boolean hasColumns = !CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns());
    final boolean hasFormat = table.inputFormatMap != null;
    if (hasColumns && !hasFormat) {
      throw new IAE("If an external table provides columns, it must also provide a format");
    }
    if (!hasColumns && hasFormat) {
      throw new IAE("If an external table provides a format, it must also provide columns");
    }
    super.validate(table);
  }

  @Override
  protected AdHocTableFunction defineAdHocTableFunction()
  {
    List<ParameterDefn> fullTableParams = adHocTableFnParameters();
    List<ParameterDefn> allParams = addFormatParameters(fullTableParams);
    return new AdHocTableFunction(allParams);
  }

  /**
   * Overridden by subclasses to provide the list of table function parameters for
   * this specific input format. This list is combined with parameters for input
   * formats. The method is called only once per run.
   */
  protected abstract List<ParameterDefn> adHocTableFnParameters();

  /**
   * Add format properties to the base set, in the order of the formats,
   * in the order defined by the format. Allow same-named properties across
   * formats, as long as the types are the same.
   */
  protected List<ParameterDefn> addFormatParameters(
      final List<ParameterDefn> properties
  )
  {
    final List<ParameterDefn> toAdd = new ArrayList<>();
    // While the format parameter is required, we mark it as optional. Else
    // if the source defines optional parameters, they will still be ignored
    // as Calcite treats (optional, optional, required) as (required, required, required)
    final ParameterDefn formatProp = new Parameter(FORMAT_PARAMETER, ParameterType.VARCHAR, true);
    toAdd.add(formatProp);
    final Map<String, ParameterDefn> formatProps = new HashMap<>();
    for (InputFormatDefn format : formats.values()) {
      for (ParameterDefn prop : format.parameters()) {
        final ParameterDefn existing = formatProps.putIfAbsent(prop.name(), prop);
        if (existing == null) {
          toAdd.add(prop);
        } else if (existing.type() != prop.type()) {
          throw new ISE(
              "Format [%s], property [%s] of class [%s] conflicts with another format property of class [%s]",
              format.typeValue(),
              prop.name(),
              prop.type().sqlName(),
              existing.type().sqlName()
          );
        }
      }
    }
    return CatalogUtils.concatLists(properties, toAdd);
  }

  @Override
  protected InputFormat convertTableToFormat(ResolvedExternalTable table)
  {
    final String formatTag = CatalogUtils.getString(table.inputFormatMap, InputFormat.TYPE_PROPERTY);
    if (formatTag == null) {
      throw new IAE("[%s] property must be provided", InputFormat.TYPE_PROPERTY);
    }
    final InputFormatDefn formatDefn = formats.get(formatTag);
    if (formatDefn == null) {
      throw new IAE(
          "Format type [%s] for property [%s] is not valid",
          formatTag,
          InputFormat.TYPE_PROPERTY
      );
    }
    return formatDefn.convertFromTable(table);
  }

  @Override
  protected InputFormat convertArgsToFormat(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
  {
    final String formatTag = CatalogUtils.getString(args, FORMAT_PARAMETER);
    if (formatTag == null) {
      throw new IAE("Must provide a value for the [%s] parameter", FORMAT_PARAMETER);
    }
    final InputFormatDefn formatDefn = formats.get(formatTag);
    if (formatDefn == null) {
      throw new IAE(
          "Format type [%s] for property [%s] is not valid",
          formatTag,
          FORMAT_PARAMETER
      );
    }
    return formatDefn.convertFromArgs(args, columns, jsonMapper);
  }

  /**
   * Converted a formatted external table given the table definition, function
   * args, columns and the merged generic JSON map representing the input source.
   *
   * @param table     the resolved external table from the catalog
   * @param args      values of arguments from an SQL table function. Here we consider
   *                  only the format arguments; input source arguments should already
   *                  have been handled
   * @param columns   the set of columns provided by the SQL table function
   * @param sourceMap the generic JSON map for the input source with function
   *                  parameters merged into the definition in the catalog
   *
   * @return          an external table spec to be used to create a Calcite table
   */
  protected ExternalTableSpec convertPartialFormattedTable(
      final ResolvedExternalTable table,
      final Map<String, Object> args,
      final List<ColumnSpec> columns,
      final Map<String, Object> sourceMap
  )
  {
    final ObjectMapper jsonMapper = table.resolvedTable().jsonMapper();

    // Choose table or SQL-provided columns: table takes precedence.
    final List<ColumnSpec> completedCols = selectPartialTableColumns(table, columns);

    // Get the format from the table, if defined, else from arguments.
    final InputFormat inputFormat;
    if (table.inputFormatMap == null) {
      inputFormat = convertArgsToFormat(args, completedCols, jsonMapper);
    } else {
      inputFormat = convertTableToFormat(table);
    }

    return new ExternalTableSpec(
        convertSource(sourceMap, jsonMapper),
        inputFormat,
        Columns.convertSignature(completedCols),
        () -> Collections.singleton(typeValue())
    );
  }
}
