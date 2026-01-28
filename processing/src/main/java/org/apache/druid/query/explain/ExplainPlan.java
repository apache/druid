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

package org.apache.druid.query.explain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.io.IOException;
import java.util.Objects;

/**
 * Class that encapsulates the information of a single plan for an {@code EXPLAIN PLAN FOR} query.
 * <p>
 * Similar to {@link #getAttributes()}, it's possible to provide more structure to {@link #getPlan()},
 * at least for the native query explain, but there's currently no use case for it.
 * </p>
 */
public class ExplainPlan
{
  @JsonProperty("PLAN")
  private final String plan;

  @JsonProperty("RESOURCES")
  private final String resources;

  @JsonProperty("ATTRIBUTES")
  @JsonDeserialize(using = ExplainAttributesDeserializer.class)
  private final ExplainAttributes attributes;

  @JsonCreator
  public ExplainPlan(
      @JsonProperty("PLAN") final String plan,
      @JsonProperty("RESOURCES") final String resources,
      @JsonProperty("ATTRIBUTES") final ExplainAttributes attributes
  )
  {
    this.plan = plan;
    this.resources = resources;
    this.attributes = attributes;
  }

  public String getPlan()
  {
    return plan;
  }

  public String getResources()
  {
    return resources;
  }

  public ExplainAttributes getAttributes()
  {
    return attributes;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExplainPlan that = (ExplainPlan) o;
    return Objects.equals(plan, that.plan)
           && Objects.equals(resources, that.resources)
           && Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(plan, resources, attributes);
  }

  /**
   * Custom deserializer for {@link ExplainAttributes} because the value for {@link #attributes} in the plan
   * is encoded as a JSON string. This deserializer tells Jackson on how to parse the JSON string
   * and map it to the fields in the {@link ExplainAttributes} class.
   */
  private static class ExplainAttributesDeserializer extends JsonDeserializer<ExplainAttributes>
  {
    @Override
    public ExplainAttributes deserialize(JsonParser jsonParser, DeserializationContext context) throws IOException
    {
      final ObjectMapper objectMapper = (ObjectMapper) jsonParser.getCodec();
      return objectMapper.readValue(jsonParser.getText(), ExplainAttributes.class);
    }
  }
}


