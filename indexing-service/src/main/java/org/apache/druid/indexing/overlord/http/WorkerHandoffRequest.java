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
 * specific information governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Optional body for {@code POST /druid/indexer/v1/worker/{host}/handoff}.
 * An omitted or empty {@code types} list means every early-handoff-capable
 * task type on that worker.
 */
public class WorkerHandoffRequest
{
  private final List<String> types;

  @JsonCreator
  public WorkerHandoffRequest(@JsonProperty("types") @Nullable List<String> types)
  {
    this.types = types;
  }

  @JsonProperty
  @Nullable
  public List<String> getTypes()
  {
    return types;
  }
}
