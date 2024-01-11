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

package org.apache.druid.catalog.model.facade;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ObjectFacade;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.segment.column.ColumnType;

import java.util.List;
import java.util.Map;

/**
 * Convenience wrapper on top of a resolved table (a table spec
 * and its corresponding definition.)
 */
public class TableFacade extends ObjectFacade
{
  protected final ResolvedTable resolved;

  public TableFacade(ResolvedTable resolved)
  {
    this.resolved = resolved;
  }

  public TableSpec spec()
  {
    return resolved.spec();
  }

  @Override
  public Map<String, Object> properties()
  {
    return spec().properties();
  }

  public List<ColumnSpec> columns()
  {
    return spec().columns();
  }

  public static ColumnType druidType(ColumnSpec col)
  {
    return Columns.druidType(col);
  }

  public ObjectMapper jsonMapper()
  {
    return resolved.jsonMapper();
  }
}
