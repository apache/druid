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

import com.google.common.collect.Lists;
import org.apache.druid.catalog.model.table.AbstractDatasourceDefn;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.server.security.ResourceType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Hard-coded schema registry that knows about the well-known, and
 * a few obscure, Druid schemas. Does not allow for user-defined
 * schemas, which the rest of Druid would not be able to support.
 */
public class SchemaRegistryImpl implements SchemaRegistry
{
  // Mimics the definition in ExternalOperatorConvertion
  // TODO: Change this when ExternalOperatorConvertion changes
  private String EXTERNAL_RESOURCE = "EXTERNAL";

  public static class SchemaDefnImpl implements SchemaSpec
  {
    private final String name;
    private final String resource;
    private final Set<String> accepts;

    public SchemaDefnImpl(
        String name,
        String resource,
        Set<String> accepts
    )
    {
      this.name = name;
      this.resource = resource;
      this.accepts = accepts;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public String securityResource()
    {
      return resource;
    }

    @Override
    public boolean writable()
    {
      return accepts != null && !accepts.isEmpty();
    }

    @Override
    public boolean accepts(String tableType)
    {
      if (accepts == null) {
        return false;
      }
      return accepts.contains(tableType);
    }
  }

  private final Map<String, SchemaSpec> builtIns;

  public SchemaRegistryImpl()
  {
    builtIns = new HashMap<>();
    register(new SchemaDefnImpl(
        TableId.DRUID_SCHEMA,
        ResourceType.DATASOURCE,
        AbstractDatasourceDefn.tableTypes()
    ));
    register(new SchemaDefnImpl(
        TableId.LOOKUP_SCHEMA,
        ResourceType.CONFIG,
        null // TODO
    ));
    register(new SchemaDefnImpl(
        TableId.CATALOG_SCHEMA,
        ResourceType.SYSTEM_TABLE,
        null
    ));
    register(new SchemaDefnImpl(
        TableId.SYSTEM_SCHEMA,
        ResourceType.SYSTEM_TABLE,
        null
    ));
    register(new SchemaDefnImpl(
        TableId.EXTERNAL_SCHEMA,
        EXTERNAL_RESOURCE,
        ExternalTableDefn.tableTypes()
    ));
    register(new SchemaDefnImpl(
        TableId.VIEW_SCHEMA,
        ResourceType.VIEW,
        null // TODO
    ));
  }

  private void register(SchemaSpec schemaDefn)
  {
    builtIns.put(schemaDefn.name(), schemaDefn);
  }

  @Override
  public SchemaSpec schema(String name)
  {
    return builtIns.get(name);
  }

  @Override
  public Set<String> names()
  {
    return new TreeSet<String>(builtIns.keySet());
  }

  @Override
  public List<SchemaSpec> schemas()
  {
    // No real need to sort every time. However, this is used infrequently,
    // so OK for now.
    List<SchemaSpec> schemas = Lists.newArrayList(builtIns.values());
    Collections.sort(schemas, (s1, s2) -> s1.name().compareTo(s2.name()));
    return schemas;
  }
}
