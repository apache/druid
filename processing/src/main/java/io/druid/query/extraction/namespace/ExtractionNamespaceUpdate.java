/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.query.extraction.namespace;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 *
 */
public class ExtractionNamespaceUpdate
{
  @JsonProperty
  private final Long updateMs;
  @JsonProperty
  private final ExtractionNamespace namespace;
  @JsonCreator
  public ExtractionNamespaceUpdate(
      @NotNull @JsonProperty(value = "namespace", required = true)
      ExtractionNamespace namespace,
      @Min(0) @Nullable @JsonProperty("updateMs")
      Long updateMs
  ){
    Preconditions.checkNotNull(namespace);
    this.updateMs = updateMs == null ? 0 : updateMs;
    this.namespace = namespace;
  }

  public Long getUpdateMs()
  {
    return updateMs;
  }

  public ExtractionNamespace getNamespace()
  {
    return namespace;
  }
  @Override
  public String toString(){
    final StringBuilder sb = new StringBuilder();
    sb.append("ExtractionNamespaceUpdate {");
    sb.append("namespace = ");
    sb.append(namespace.toString());
    sb.append(',');
    sb.append("updateMs = ");
    sb.append(updateMs);
    sb.append("}");
    return sb.toString();
  }
}
