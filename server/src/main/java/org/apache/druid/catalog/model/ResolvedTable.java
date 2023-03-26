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

import java.util.Map;

/**
 * Handle to a table specification along with its definition
 * and the object mapper used to serialize/deserialize its data.
 * This class provides the semantic view of the table specification,
 * without the operational aspects such as version, name, etc.
 *
 * @see {@link TableMetadata} for the operational representation
 */
public class ResolvedTable extends ObjectFacade
{
  private final TableDefn defn;
  private final TableSpec spec;
  private final ObjectMapper jsonMapper;

  public ResolvedTable(
      final TableDefn defn,
      final TableSpec spec,
      final ObjectMapper jsonMapper
  )
  {
    this.defn = defn;
    this.spec = spec;
    this.jsonMapper = jsonMapper;
  }

  public TableDefn defn()
  {
    return defn;
  }

  public TableSpec spec()
  {
    return spec;
  }

  public ResolvedTable merge(TableSpec update)
  {
    return new ResolvedTable(
        defn,
        defn.merge(spec, update, jsonMapper),
        jsonMapper
    );
  }

  public ResolvedTable withProperties(Map<String, Object> props)
  {
    return new ResolvedTable(defn, spec.withProperties(props), jsonMapper);
  }

  public void validate()
  {
    spec.validate();
    defn.validate(this);
  }

  @Override
  public Map<String, Object> properties()
  {
    return spec.properties();
  }

  public ObjectMapper jsonMapper()
  {
    return jsonMapper;
  }
}
