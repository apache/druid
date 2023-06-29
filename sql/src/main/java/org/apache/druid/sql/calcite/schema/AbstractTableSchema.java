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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.SchemaVersion;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * Calcite provides two "schema" abstractions. {@link Schema} is an interface,
 * while {@link org.apache.calcite.schema.impl.AbstractSchema} is a base class
 * that "locks down" the {@link #getTable} method to obtain the table from a
 * map. This forces the extensions of that class to materialize all tables in
 * the schema, so that Calcite can pick the one it wants. This class, by
 * contrast, provides the same defaults as {@link
 * org.apache.calcite.schema.impl.AbstractSchema AbstractSchema}, but assumes
 * its subslasses will implement {@code getTable()} to directly look up that
 * one table, ignoring all others. Doing so lowers the cost of table resolution,
 * especially when the system has to fetch catalog information for the table:
 * we only fetch the information we need, not information for all tables.
 */
public abstract class AbstractTableSchema implements Schema
{
  protected static final Set<String> EMPTY_NAMES = ImmutableSet.of();

  @Override
  public RelProtoDataType getType(String name)
  {
    return null;
  }

  @Override
  public Set<String> getTypeNames()
  {
    return EMPTY_NAMES;
  }

  @Override
  public Collection<Function> getFunctions(String name)
  {
    return Collections.emptyList();
  }

  @Override
  public Set<String> getFunctionNames()
  {
    return EMPTY_NAMES;
  }

  @Override
  public Schema getSubSchema(String name)
  {
    return null;
  }

  @Override
  public Set<String> getSubSchemaNames()
  {
    return EMPTY_NAMES;
  }

  @Override
  public Expression getExpression(SchemaPlus parentSchema, String name)
  {
    return null;
  }

  @Override
  public boolean isMutable()
  {
    return false;
  }

  @Override
  public Schema snapshot(SchemaVersion version)
  {
    return this;
  }
}
