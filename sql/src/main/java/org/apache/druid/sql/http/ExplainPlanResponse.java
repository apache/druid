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


package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ExplainPlanResponse
{
  @JsonProperty("PLAN")
  private final String plan;
  @JsonProperty("RESOURCES")
  private final String resources;
  // TODO: investigate why changing the type from String to ExplainAttributes doesn't work.
  @JsonProperty("ATTRIBUTES")
  private final String attributes;

  @JsonCreator
  public ExplainPlanResponse(
      @JsonProperty("PLAN") String plan,
      @JsonProperty("RESOURCES") String resources,
      @JsonProperty("ATTRIBUTES") String attributes
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

  public String getAttributes()
  {
    return attributes;
  }
}

