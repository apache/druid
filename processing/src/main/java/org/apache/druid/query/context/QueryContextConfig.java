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

package org.apache.druid.query.context;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Runtime configuration for validating query contexts, bound to the {@code druid.query.context} property prefix.
 */
public class QueryContextConfig
{
  public static final QueryContextConfig DEFAULT = new QueryContextConfig(null);

  /**
   * Whether to reject query contexts that contain a parameter that is not declared in {@link QueryContextParameters}:
   * the contexts of queries on the Broker, and the default contexts where they are configured. Off by default, so that
   * queries with such parameters keep working.
   */
  @JsonProperty
  private final boolean rejectUnknownParameters;

  @JsonCreator
  public QueryContextConfig(@JsonProperty("rejectUnknownParameters") @Nullable final Boolean rejectUnknownParameters)
  {
    this.rejectUnknownParameters = rejectUnknownParameters != null && rejectUnknownParameters;
  }

  public boolean isRejectUnknownParameters()
  {
    return rejectUnknownParameters;
  }
}
