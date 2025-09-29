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

import com.google.inject.Inject;
import org.apache.calcite.schema.Schema;
import org.apache.druid.catalog.model.TableId;

/**
 * Makes indexing template definitions accessible via SQL.
 */
public class NamedIndexingTemplateSchema implements NamedSchema
{
  private final IndexingTemplateSchema schema;

  @Inject
  public NamedIndexingTemplateSchema(IndexingTemplateSchema schema)
  {
    this.schema = schema;
  }

  @Override
  public String getSchemaName()
  {
    return TableId.INDEXING_TEMPLATE_SCHEMA;
  }

  @Override
  public Schema getSchema()
  {
    return schema;
  }
}
