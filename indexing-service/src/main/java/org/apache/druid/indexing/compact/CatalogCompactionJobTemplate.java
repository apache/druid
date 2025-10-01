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

package org.apache.druid.indexing.compact;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.IndexingTemplateDefn;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.input.DruidInputSource;
import org.apache.druid.indexing.template.BatchIndexingJobTemplate;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Compaction template that delegates job creation to a template stored in the
 * Druid catalog.
 */
public class CatalogCompactionJobTemplate implements CompactionJobTemplate
{
  public static final String TYPE = "compactCatalog";

  private final String templateId;

  private final TableId tableId;
  private final MetadataCatalog catalog;

  @JsonCreator
  public CatalogCompactionJobTemplate(
      @JsonProperty("templateId") String templateId,
      @JacksonInject MetadataCatalog catalog
  )
  {
    this.templateId = templateId;
    this.catalog = catalog;
    this.tableId = TableId.of(TableId.INDEXING_TEMPLATE_SCHEMA, templateId);
  }

  @JsonProperty
  public String getTemplateId()
  {
    return templateId;
  }

  @Nullable
  @Override
  public Granularity getSegmentGranularity()
  {
    return getDelegate().getSegmentGranularity();
  }

  @Override
  public List<CompactionJob> createCompactionJobs(
      DruidInputSource source,
      CompactionJobParams params
  )
  {
    return getDelegate().createCompactionJobs(source, params);
  }

  private CompactionJobTemplate getDelegate()
  {
    final ResolvedTable resolvedTable = catalog.resolveTable(tableId);
    if (resolvedTable == null) {
      throw InvalidInput.exception("Could not find table[%s] in the catalog", tableId);
    }

    final BatchIndexingJobTemplate delegate
        = resolvedTable.decodeProperty(IndexingTemplateDefn.PROPERTY_PAYLOAD);
    if (delegate instanceof CompactionJobTemplate) {
      return (CompactionJobTemplate) delegate;
    } else {
      throw InvalidInput.exception(
          "Template[%s] of type[%s] cannot be used for creating compaction tasks",
          templateId, delegate == null ? null : delegate.getType()
      );
    }
  }

  @Override
  public String getType()
  {
    return TYPE;
  }
}
