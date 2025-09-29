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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.catalog.model.ModelProperties;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.indexing.template.BatchIndexingJobTemplate;

import java.util.List;

/**
 * Definition for indexing templates.
 */
public class IndexingTemplateDefn extends TableDefn
{
  public static final String TYPE = "index_template";

  /**
   * Property to contain template payload of type {@link BatchIndexingJobTemplate}.
   *
   * @see PayloadProperty#TYPE_REF
   */
  public static final String PROPERTY_PAYLOAD = "payload";

  public IndexingTemplateDefn()
  {
    super(
        "Ingestion Template",
        TYPE,
        List.of(new PayloadProperty()),
        null
    );
  }

  /**
   * Template payload property.
   */
  public static class PayloadProperty extends ModelProperties.TypeRefPropertyDefn<BatchIndexingJobTemplate>
  {
    public static final TypeReference<BatchIndexingJobTemplate> TYPE_REF = new TypeReference<>() {};

    public PayloadProperty()
    {
      super(PROPERTY_PAYLOAD, "Payload of the batch indexing template", TYPE_REF);
    }
  }
}
