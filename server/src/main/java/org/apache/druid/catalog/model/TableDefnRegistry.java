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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.table.DatasourceDefn;
import org.apache.druid.catalog.model.table.HttpTableDefn;
import org.apache.druid.catalog.model.table.InlineTableDefn;
import org.apache.druid.catalog.model.table.LocalTableDefn;
import org.apache.druid.java.util.common.IAE;

import java.util.Map;

public class TableDefnRegistry
{
  private static final TableDefn[] TABLE_DEFNS = {
      new DatasourceDefn(),
      new InlineTableDefn(),
      new HttpTableDefn(),
      new LocalTableDefn()
  };

  private final Map<String, TableDefn> defns;
  private final ObjectMapper jsonMapper;

  public TableDefnRegistry(
      final TableDefn[] defns,
      final ObjectMapper jsonMapper
  )
  {
    ImmutableMap.Builder<String, TableDefn> builder = ImmutableMap.builder();
    for (TableDefn defn : defns) {
      builder.put(defn.typeValue(), defn);
    }
    this.defns = builder.build();
    this.jsonMapper = jsonMapper;
  }

  public TableDefnRegistry(
      final ObjectMapper jsonMapper
  )
  {
    this(TABLE_DEFNS, jsonMapper);
  }

  public TableDefn defnFor(String type)
  {
    return defns.get(type);
  }

  public ResolvedTable resolve(TableSpec spec)
  {
    String type = spec.type();
    if (Strings.isNullOrEmpty(type)) {
      throw new IAE("The table type is required.");
    }
    TableDefn defn = defns.get(type);
    if (defn == null) {
      throw new IAE("Table type [%s] is not valid.", type);
    }
    return new ResolvedTable(defn, spec, jsonMapper);
  }
}
