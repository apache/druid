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

/**
 * Registry of the table types supported in the catalog. This registry
 * is used to validate insertions and updates. A table spec has a type field.
 * That spec is said to be "resolved" when we use that type field to look up
 * the definition for that type, and combine them into a {@link ResolvedTable}.
 * <p>
 * Note an inherent race condition:
 * once a record is written into the metadata DB, that record has a life
 * independent of what happens in this table. It may be that a catalog entry
 * is created for a table type defined in an extension. Later, that extension
 * is removed. The system must still work with the now "unknown" table types
 * in the DB. But, we won't allow the use of, changes to, or new instances of
 * that type. The choice is to delete the now undefined table, or restore the
 * extension.
 * <p>
 * Holds onto the JSON mapper to simplify the resolution process: the
 * {@link ResolvedTable} provides the mapper used to serialize the table spec.
 */
public class TableDefnRegistry
{
  // Temporary list of Druid-define table definitions. This should come from
  // Guice later to allow extensions to define table types.
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

  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
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
