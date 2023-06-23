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

import com.google.common.base.Strings;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.BaseTableFunction.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterType;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Definition for a {@link LocalInputSource}.
 */
public class LocalInputSourceDefn extends FormattedInputSourceDefn
{
  public static final String TYPE_KEY = LocalInputSource.TYPE_KEY;

  /**
   * Base directory for file or filter operations. If not provided,
   * then the servers current working directory is assumed, which is
   * typically valid only for sample data.
   */
  public static final String BASE_DIR_PARAMETER = "baseDir";

  public static final String FILTER_PARAMETER = "filter";
  public static final String FILES_PARAMETER = "files";

  protected static final String BASE_DIR_FIELD = "baseDir";
  protected static final String FILES_FIELD = "files";
  protected static final String FILTER_FIELD = "filter";

  private static final ParameterDefn FILTER_PARAM_DEFN =
      new Parameter(FILTER_PARAMETER, ParameterType.VARCHAR, true);
  private static final ParameterDefn FILES_PARAM_DEFN =
      new Parameter(FILES_PARAMETER, ParameterType.VARCHAR_ARRAY, true);

  @Override
  public String typeValue()
  {
    return LocalInputSource.TYPE_KEY;
  }

  @Override
  protected Class<? extends InputSource> inputSourceClass()
  {
    return LocalInputSource.class;
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.inputSourceMap);
    final boolean hasBaseDir = sourceMap.containsKey(BASE_DIR_FIELD);
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));

    if (!hasBaseDir && !hasFiles) {
      throw new IAE(
          "A local input source requires one property of %s or %s",
          BASE_DIR_FIELD,
          FILES_FIELD
      );
    }
    if (!hasBaseDir && hasFilter) {
      throw new IAE(
          "If a local input source sets property %s, it must also set property %s",
          FILTER_FIELD,
          BASE_DIR_FIELD
      );
    }
    if (hasBaseDir && hasFiles) {
      throw new IAE(
          "A local input source accepts only one of %s or %s",
          BASE_DIR_FIELD,
          FILES_FIELD
      );
    }
    super.validate(table);
  }

  @Override
  protected List<ParameterDefn> adHocTableFnParameters()
  {
    return Arrays.asList(
        new Parameter(BASE_DIR_PARAMETER, ParameterType.VARCHAR, true),
        FILTER_PARAM_DEFN,
        FILES_PARAM_DEFN
    );
  }

  @Override
  protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    jsonMap.put(InputSource.TYPE_PROPERTY, LocalInputSource.TYPE_KEY);

    final String baseDirParam = CatalogUtils.getString(args, BASE_DIR_PARAMETER);
    final List<String> filesParam = CatalogUtils.getStringArray(args, FILES_PARAMETER);
    final String filterParam = CatalogUtils.getString(args, FILTER_PARAMETER);
    final boolean hasBaseDir = !Strings.isNullOrEmpty(baseDirParam);
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(filesParam);
    final boolean hasFilter = !Strings.isNullOrEmpty(filterParam);
    if (!hasBaseDir && !hasFiles) {
      throw new IAE(
          "A local input source requires one parameter of %s or %s",
          BASE_DIR_PARAMETER,
          FILES_PARAMETER
      );
    }
    if (hasBaseDir && hasFiles) {
      if (hasFilter) {
        throw new IAE(
            "A local input source can set parameter %s or %s, but not both.",
            FILES_PARAMETER,
            FILTER_PARAMETER
        );
      }
      // Special workaround: if the user provides a base dir, and files, convert
      // the files to be absolute paths relative to the base dir. Then, remove
      // the baseDir, since the Druid input source does not allow both a baseDir
      // and a list of files.
      jsonMap.put(FILES_FIELD, absolutePath(baseDirParam, filesParam));
      return;
    }
    if (!hasBaseDir && !Strings.isNullOrEmpty(filterParam)) {
      throw new IAE(
          "If a local input source sets parameter %s, it must also set parameter %s",
          FILTER_PARAMETER,
          BASE_DIR_PARAMETER
      );
    }
    if (hasBaseDir) {
      jsonMap.put(BASE_DIR_FIELD, baseDirParam);
    }
    if (hasFiles) {
      jsonMap.put(FILES_FIELD, filesParam);
    }
    if (filterParam != null) {
      jsonMap.put(FILTER_FIELD, filterParam);
    }
  }

  private List<String> absolutePath(String baseDirPath, List<String> files)
  {
    final File baseDir = new File(baseDirPath);
    return files.stream()
        .map(f -> new File(baseDir, f).toString())
        .collect(Collectors.toList());
  }

  @Override
  public TableFunction partialTableFn(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.inputSourceMap);
    final boolean hasBaseDir = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, BASE_DIR_FIELD));
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));
    List<ParameterDefn> params = new ArrayList<>();
    if (hasBaseDir && !hasFiles && !hasFilter) {
      params.add(FILES_PARAM_DEFN);
      params.add(FILTER_PARAM_DEFN);
    }

    // Does the table define a format?
    if (table.inputFormatMap == null) {
      params = addFormatParameters(params);
    }
    return new PartialTableFunction(table, params);
  }

  @Override
  protected ExternalTableSpec convertCompletedTable(
      final ResolvedExternalTable table,
      final Map<String, Object> args,
      final List<ColumnSpec> columns
  )
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.inputSourceMap);
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    if (hasFiles) {
      if (!args.isEmpty()) {
        throw new IAE("The local input source has a file list: do not provide other arguments");
      }
    } else {
      final String baseDir = CatalogUtils.getString(sourceMap, BASE_DIR_FIELD);
      if (Strings.isNullOrEmpty(baseDir)) {
        throw new IAE(
            "When a local external table is used with a table function, %s must be set",
            BASE_DIR_FIELD
        );
      }
      final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));
      final List<String> filesParam = CatalogUtils.getStringArray(args, FILES_PARAMETER);
      final boolean hasFilesParam = !CollectionUtils.isNullOrEmpty(filesParam);
      final String filterParam = CatalogUtils.getString(args, FILTER_PARAMETER);
      final boolean hasFilterParam = !Strings.isNullOrEmpty(filterParam);
      if (!hasFilter && !hasFilesParam && !hasFilterParam) {
        throw new IAE(
            "For a local input source, set either %s or %s",
            FILES_PARAMETER,
            FILTER_PARAMETER
        );
      }
      if (hasFilesParam) {
        // Special workaround: if the user provides a base dir, and files, convert
        // the files to be absolute paths relative to the base dir. Then, remove
        // the baseDir, since the Druid input source does not allow both a baseDir
        // and a list of files.
        sourceMap.remove(FILTER_FIELD);
        sourceMap.remove(BASE_DIR_FIELD);
        sourceMap.put(FILES_FIELD, absolutePath(baseDir, filesParam));
      } else if (filterParam != null) {
        sourceMap.put(FILTER_FIELD, filterParam);
      }
    }
    return convertPartialFormattedTable(table, args, columns, sourceMap);
  }

  @Override
  protected void auditInputSource(Map<String, Object> jsonMap)
  {
    // Note the odd semantics of this class.
    // If we give a base directory, and explicitly state files, we must
    // also provide a file filter which presumably matches the very files
    // we list. Take pity on the user and provide a filter in this case.
    String filter = CatalogUtils.getString(jsonMap, FILTER_FIELD);
    if (filter != null) {
      return;
    }
    String baseDir = CatalogUtils.getString(jsonMap, BASE_DIR_FIELD);
    if (baseDir != null) {
      jsonMap.put(FILTER_FIELD, "*");
    }
  }

  @Override
  public ExternalTableSpec convertTable(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.inputSourceMap);
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));
    if (!hasFiles && !hasFilter) {
      throw new IAE(
          "Use a table function to set either %s or %s",
          FILES_PARAMETER,
          FILTER_PARAMETER
      );
    }
    return super.convertTable(table);
  }
}
