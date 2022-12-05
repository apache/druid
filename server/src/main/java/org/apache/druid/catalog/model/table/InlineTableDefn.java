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
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Describes an inline table: one where the data is provided in the
 * table spec as a series of text lines.
 */
public class InlineTableDefn extends FormattedExternalTableDefn
{
  public static final String TABLE_TYPE = InlineInputSource.TYPE_KEY;
  public static final String DATA_PROPERTY = "data";

  public InlineTableDefn()
  {
    super(
        "Inline input table",
        TABLE_TYPE,
        Collections.singletonList(
            new StringListPropertyDefn(DATA_PROPERTY)
        ),
        Collections.singletonList(INPUT_COLUMN_DEFN),
        InputFormats.ALL_FORMATS,
        null
    );
  }

  @Override
  protected InputSource convertSource(ResolvedTable table)
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put(InputSource.TYPE_PROPERTY, InlineInputSource.TYPE_KEY);
    List<String> dataList = table.stringListProperty(DATA_PROPERTY);

    // Would be nice, from a completeness perspective, for the inline data
    // source to allow zero rows of data. However, such is not the case.
    if (CollectionUtils.isNullOrEmpty(dataList)) {
      throw new IAE(
          "An inline table requires one or more rows of data in the '%s' property",
          DATA_PROPERTY
      );
    }
    jsonMap.put("data", CatalogUtils.stringListToLines(dataList));
    return convertObject(table.jsonMapper(), jsonMap, InlineInputSource.class);
  }

  @Override
  public ResolvedTable mergeParameters(ResolvedTable spec, Map<String, Object> values)
  {
    throw new UOE("Inline table does not support parameters");
  }

  @Override
  public void validate(ResolvedTable table)
  {
    super.validate(table);
    convertSource(table);
  }
}
