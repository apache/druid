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
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;

import java.util.HashMap;
import java.util.Map;

/**
 * Internal class to hold the intermediate form of an external table: the
 * input source and input format properties converted to Java maps, and the
 * types of each resolved to the corresponding definitions. Used to validate
 * a table specification, and to convert a table specification to an
 * {@link ExternalTableSpec} when used in SQL.
 */
public class ResolvedExternalTable
{
  private final ResolvedTable table;
  protected final Map<String, Object> inputSourceMap;
  protected final Map<String, Object> inputFormatMap;
  private InputSourceDefn inputSourceDefn;
  private InputFormatDefn inputFormatDefn;

  /**
   * Construct a resolved external table by extracting the input source
   * and input format properties, and converting each to a Java map.
   * Validates that the input source is present: the format is optional.
   * <p>
   * Note: does <i>not</i> resolve the input source and input format
   * definitions: that is done as a separate step when needed.
   *
   * @see {@link #resolve(TableDefnRegistry)}.
   */
  public ResolvedExternalTable(final ResolvedTable table)
  {
    this.table = table;
    Map<String, Object> map = table.mapProperty(ExternalTableDefn.SOURCE_PROPERTY);
    if (map == null || map.isEmpty()) {
      throw new IAE("%s property is required", ExternalTableDefn.SOURCE_PROPERTY);
    }
    this.inputSourceMap = new HashMap<>(map);
    map = table.mapProperty(ExternalTableDefn.FORMAT_PROPERTY);
    this.inputFormatMap = map == null ? null : new HashMap<>(map);
  }

  public ResolvedTable resolvedTable()
  {
    return table;
  }

  /**
   * Look up the input source type and input format type to find the corresponding
   * definitions in the table registry. Throws an exception if the types are not
   * defined. The input source is required, the format is optional.
   * <p>
   * Note, for resolution to work, the name of each definition must be the same as
   * that used as the type key in the serialized JSON.
   */
  public ResolvedExternalTable resolve(TableDefnRegistry registry)
  {
    String inputSourceType = CatalogUtils.getString(inputSourceMap, InputSource.TYPE_PROPERTY);
    if (inputSourceType == null) {
      throw new IAE("Input source type %s is required", InputSource.TYPE_PROPERTY);
    }
    inputSourceDefn = registry.inputSourceDefnFor(inputSourceType);
    if (inputSourceDefn == null) {
      throw new IAE("Input source type %s is not registered", inputSourceType);
    }
    if (inputFormatMap != null) {
      String inputFormatType = CatalogUtils.getString(inputFormatMap, InputFormat.TYPE_PROPERTY);
      if (inputFormatType != null) {
        inputFormatDefn = registry.inputFormatDefnFor(inputFormatType);
        if (inputFormatDefn == null) {
          throw new IAE("Input format type %s is not registered", inputFormatType);
        }
      }
    }
    return this;
  }

  /**
   * Validate that the table spec is correct by resolving the definitions, then
   * converting the JSON to the desired object type. Note that this path requires
   * special handling: the table spec may be partial, which means it is missing information
   * needed to create a complete input source. The input source definition defines which
   * values can be omitted, and defined later in SQL via function parameters. If those
   * values are missing, then the input source defn should provide dummy values so that
   * the validation will succeed (assuming that the properties that are provided are valid.)
   */
  public void validate(TableDefnRegistry registry)
  {
    resolve(registry);
    inputSourceDefn.validate(this);
    if (inputFormatDefn != null) {
      inputFormatDefn.validate(this);
    }
  }

  /**
   * Return a table function definition for a partial table as given by
   * this object. The function defines parameters to gather the
   * values needed to convert the partial table into a fully-defined table
   * which can be converted to an {@link ExternalTableSpec}.
   */
  public TableFunction tableFn()
  {
    return inputSourceDefn.partialTableFn(this);
  }

  /**
   * Return the {@link ExternalTableSpec} for a catalog entry for a this object
   * which must be a fully-defined table.
   */
  public ExternalTableSpec convert()
  {
    return inputSourceDefn.convertTable(this);
  }
}
