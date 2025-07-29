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

package org.apache.druid.catalog;

import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class NullMetadataCatalog implements MetadataCatalog
{
  public static final MetadataCatalog INSTANCE = new NullMetadataCatalog();

  @Nullable
  @Override
  public TableMetadata getTable(TableId tableId)
  {
    return null;
  }

  @Nullable
  @Override
  public ResolvedTable resolveTable(TableId tableId)
  {
    return null;
  }

  @Override
  public List<TableMetadata> tables(String schemaName)
  {
    return Collections.emptyList();
  }

  @Override
  public Set<String> tableNames(String schemaName)
  {
    return Collections.emptySet();
  }
}
