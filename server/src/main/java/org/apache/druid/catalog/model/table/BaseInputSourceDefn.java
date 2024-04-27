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
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Base class for input source definitions.
 *
 * @see {@link FormattedInputSourceDefn} for the base class for (most) input formats
 * which take an input format.
 */
public abstract class BaseInputSourceDefn implements InputSourceDefn
{
  private static final Logger LOG = new Logger(BaseInputSourceDefn.class);

  /**
   * The "from-scratch" table function for this input source. The parameters
   * are those defined by the subclass, and the apply simply turns around and
   * asks the input source definition to do the conversion.
   */
  public class AdHocTableFunction extends BaseTableFunction
  {
    public AdHocTableFunction(List<ParameterDefn> parameters)
    {
      super(parameters);
    }

    @Override
    public ExternalTableSpec apply(
        final String fnName,
        final Map<String, Object> args,
        final List<ColumnSpec> columns,
        final ObjectMapper jsonMapper
    )
    {
      requireSchema(fnName, columns);
      return convertArgsToTable(args, columns, jsonMapper);
    }
  }

  /**
   * The "partial" table function that starts with a catalog external table spec, then
   * uses SQL function arguments to "complete" (i.e. fill in) the missing properties to
   * produce a complete table which is then converted to an external table which Calcite
   * can use.
   * <p>
   * The set of parameters depends on the input source and on whether or not the catalog
   * spec provides a format.
   */
  public class PartialTableFunction extends BaseTableFunction
  {
    private final ResolvedExternalTable table;

    public PartialTableFunction(final ResolvedExternalTable table, List<ParameterDefn> params)
    {
      super(params);
      this.table = table;
    }

    @Override
    public ExternalTableSpec apply(
        final String fnName,
        final Map<String, Object> args,
        final List<ColumnSpec> columns,
        final ObjectMapper jsonMapper
    )
    {
      if (CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns())) {
        requireSchema(fnName, columns);
      }
      return convertCompletedTable(table, args, columns);
    }
  }

  /**
   * The one and only from-scratch table function for this input source. The
   * function is defined a bind time, not construction time, since it typically
   * needs visibility to the set of available input formats.
   */
  private AdHocTableFunction adHocTableFn;

  /**
   * Overridden by each subclass to return the input source class to be
   * used for JSON conversions.
   */
  protected abstract Class<? extends InputSource> inputSourceClass();

  @Override
  public void bind(TableDefnRegistry registry)
  {
    this.adHocTableFn = defineAdHocTableFunction();
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    convertTableToSource(table);
  }

  /**
   * Overridden by each subclass to define the parameters needed by each
   * input source.
   */
  protected abstract AdHocTableFunction defineAdHocTableFunction();

  @Override
  public TableFunction adHocTableFn()
  {
    return adHocTableFn;
  }

  /**
   * Define a table "from scratch" using SQL function arguments.
   */
  protected ExternalTableSpec convertArgsToTable(
      final Map<String, Object> args,
      final List<ColumnSpec> columns,
      final ObjectMapper jsonMapper
  )
  {
    return new ExternalTableSpec(
        convertArgsToSource(args, jsonMapper),
        convertArgsToFormat(args, columns, jsonMapper),
        Columns.convertSignature(columns),
        () -> Collections.singleton(typeValue())
    );
  }

  /**
   * Convert the input source using arguments to a "from scratch" table function.
   */
  protected InputSource convertArgsToSource(Map<String, Object> args, ObjectMapper jsonMapper)
  {
    final Map<String, Object> jsonMap = new HashMap<>();
    auditInputSource(jsonMap);
    convertArgsToSourceMap(jsonMap, args);
    return convertSource(jsonMap, jsonMapper);
  }

  /**
   * Convert SQL arguments to the corresponding "generic JSON" form in the given map.
   * The map will then be adjusted and converted to the actual input source.
   */
  protected abstract void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args);

  /**
   * Convert SQL arguments, and the column schema, to an input format, if required.
   */
  protected InputFormat convertArgsToFormat(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
  {
    return null;
  }

  /**
   * Complete a partial table using the table function arguments and columns provided.
   * The arguments match the set of parameters used for the function. The columns are
   * provided if the SQL included an {@code EXTENDS} clause: the implementation should decide
   * if columns are required (or allowed) depending on whether the partial spec already
   * defines columns.
   *
   * @param table   the partial table spec, with input source and format parsed into a
   *                generic Java map
   * @param args    the argument values provided in the SQL table function call. The arguments
   *                use the Java types defined in the parameter definitions.
   * @param columns the set of columns (if any) from the SQL {@code EXTEND} clause
   *
   * @return an external table spec which Calcite can consume
   */
  protected abstract ExternalTableSpec convertCompletedTable(
      ResolvedExternalTable table,
      Map<String, Object> args,
      List<ColumnSpec> columns
  );

  @Override
  public ExternalTableSpec convertTable(ResolvedExternalTable table)
  {
    return new ExternalTableSpec(
        convertTableToSource(table),
        convertTableToFormat(table),
        Columns.convertSignature(table.resolvedTable().spec().columns()),
        () -> Collections.singleton(typeValue())
    );
  }

  /**
   * Converts the input source given in a table spec. Since Druid input sources
   * were not designed for the use by the catalog or SQL, some cleanup is done to
   * simplify the parameters which the user provides.
   *
   * @param table the resolved external table spec
   * @return the input source converted from the spec
   */
  protected InputSource convertTableToSource(ResolvedExternalTable table)
  {
    return convertSource(
        new HashMap<>(table.inputSourceMap),
        table.resolvedTable().jsonMapper()
    );
  }

  /**
   * Convert from a generic Java map to the target input source using the object
   * mapper provided. Translates Jackson errors into a generic unchecked error.
   */
  protected InputSource convertSource(
      final Map<String, Object> jsonMap,
      final ObjectMapper jsonMapper
  )
  {
    try {
      auditInputSource(jsonMap);
      return jsonMapper.convertValue(jsonMap, inputSourceClass());
    }
    catch (Exception e) {
      LOG.debug(e, "Invalid input source specification");
      throw new IAE(e, "Invalid input source specification");
    }
  }

  /**
   * Optional step to audit or adjust the input source properties prior to
   * conversion via Jackson. Changes are made directly in the {@code jsonMap}.
   */
  protected void auditInputSource(Map<String, Object> jsonMap)
  {
  }

  /**
   * Convert the format spec, if any, to an input format.
   */
  protected abstract InputFormat convertTableToFormat(ResolvedExternalTable table);

  /**
   *  Choose table or SQL-provided columns: table takes precedence.
   */
  protected List<ColumnSpec> selectPartialTableColumns(
      final ResolvedExternalTable table,
      final List<ColumnSpec> columns
  )
  {
    final List<ColumnSpec> tableCols = table.resolvedTable().spec().columns();
    if (CollectionUtils.isNullOrEmpty(tableCols)) {
      return columns;
    } else if (!CollectionUtils.isNullOrEmpty(columns)) {
      throw new IAE(
          "Catalog definition for the [%s] input source already contains column definitions",
          typeValue()
      );
    } else {
      return tableCols;
    }
  }
}
