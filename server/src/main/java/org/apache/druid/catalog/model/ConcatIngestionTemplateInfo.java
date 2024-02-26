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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.IAE;

import java.util.List;

@JsonTypeName("concat")
public class ConcatIngestionTemplateInfo implements IngestionTemplateInfo
{
  @JsonProperty
  private final String templateName;

  @JsonProperty
  private final String prepend;

  @JsonCreator
  public ConcatIngestionTemplateInfo(
      @JsonProperty("templateName") String templateName,
      @JsonProperty("prepend") String prepend
  )
  {
    Preconditions.checkNotNull(templateName);
    Preconditions.checkNotNull(prepend);
    this.templateName = templateName;
    this.prepend = prepend;
  }

  @Override
  public String getType()
  {
    return "concat";
  }

  @Override
  public List<String> getRequiredTemplateNames()
  {
    return ImmutableList.of(templateName);
  }

  @Override
  public String generateQueryFromTemplates(List<IngestionTemplate> templates)
  {
    if (templates.size() != 1) {
      throw new IAE("Only 1 referenced template can be used with concat templates.");
    }
    if (!(templates.get(0) instanceof ConcatIngestionTemplate)) {
      throw new IAE("Referenced template [%s] is not a concat template.", templates.get(0));
    }

    ConcatIngestionTemplate concatTemplate = (ConcatIngestionTemplate) templates.get(0);
    return prepend + concatTemplate.getBody();
  }

  @JsonProperty("templateName")
  public String getTemplateName()
  {
    return templateName;
  }

  @JsonProperty("prepend")
  public String getPrepend()
  {
    return prepend;
  }
}
