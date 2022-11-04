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
import org.apache.druid.catalog.model.ModelProperties.StringListPropertyDefn;
import org.apache.druid.catalog.model.ModelProperties.StringPropertyDefn;
import org.apache.druid.catalog.model.ParameterizedDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.utils.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Definition for a catalog table object that represents a Druid
 * {@link LocalInputSource}.
 */
public class LocalTableDefn extends FormattedExternalTableDefn implements ParameterizedDefn
{
  public static final String TABLE_TYPE = LocalInputSource.TYPE_KEY;

  /**
   * Base directory for file or filter operations. If not provided,
   * then the servers current working directory is assumed, which is
   * typically valid only for sample data.
   */
  public static final String BASE_DIR_PROPERTY = "baseDir";

  // Note name "fileFilter", not "filter". These properties mix in with
  // others and "filter" is a bit too generic in that context.
  public static final String FILE_FILTER_PROPERTY = "fileFilter";
  public static final String FILES_PROPERTY = "files";

  public LocalTableDefn()
  {
    super(
        "Local file input table",
        TABLE_TYPE,
        Arrays.asList(
            new StringPropertyDefn(BASE_DIR_PROPERTY),
            new StringPropertyDefn(FILE_FILTER_PROPERTY),
            new StringListPropertyDefn(FILES_PROPERTY)
        ),
        Collections.singletonList(INPUT_COLUMN_DEFN),
        InputFormats.ALL_FORMATS,
        Arrays.asList(
            new ParameterImpl(FILE_FILTER_PROPERTY, String.class),
            new ParameterImpl(FILES_PROPERTY, String.class)
        )
    );
  }

  @Override
  public ResolvedTable mergeParameters(ResolvedTable table, Map<String, Object> values)
  {
    // The safe get can only check
    String filesParam = CatalogUtils.safeGet(values, FILES_PROPERTY, String.class);
    String filterParam = CatalogUtils.safeGet(values, FILE_FILTER_PROPERTY, String.class);
    Map<String, Object> revisedProps = new HashMap<>(table.properties());
    if (filesParam != null) {
      revisedProps.put(FILES_PROPERTY, CatalogUtils.stringToList(filesParam));
    }
    if (filterParam != null) {
      revisedProps.put(FILE_FILTER_PROPERTY, filterParam);
    }
    return table.withProperties(revisedProps);
  }

  @Override
  protected InputSource convertSource(ResolvedTable table)
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(InputSource.TYPE_PROPERTY, LocalInputSource.TYPE_KEY);
    String baseDir = table.stringProperty(BASE_DIR_PROPERTY);
    jsonMap.put("baseDir", baseDir);
    List<String> files = table.stringListProperty(FILES_PROPERTY);
    jsonMap.put("files", files);

    // Note the odd semantics of this class.
    // If we give a base directory, and explicitly state files, we must
    // also provide a file filter which presumably matches the very files
    // we list. Take pity on the user and provide a filter in this case.
    String filter = table.stringProperty(FILE_FILTER_PROPERTY);
    if (baseDir != null && !CollectionUtils.isNullOrEmpty(files) && filter == null) {
      filter = "*";
    }
    jsonMap.put("filter", filter);
    return convertObject(table.jsonMapper(), jsonMap, LocalInputSource.class);
  }

  @Override
  public void validate(ResolvedTable table)
  {
    super.validate(table);
    formatDefn(table).validate(table);

    // Validate the source if it is complete enough; else we need
    // parameters later.
    if (table.hasProperty(BASE_DIR_PROPERTY) || table.hasProperty(FILES_PROPERTY)) {
      convertSource(table);
    }
  }
}
