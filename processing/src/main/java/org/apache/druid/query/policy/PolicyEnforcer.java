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

package org.apache.druid.query.policy;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.query.DataSource;

/**
 * A mechanism for enforcing policies on data sources in Druid queries.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = NoopPolicyEnforcer.class)
@JsonSubTypes({
    @JsonSubTypes.Type(value = NoopPolicyEnforcer.class, name = "none"),
    @JsonSubTypes.Type(value = RestrictAllTablesPolicyEnforcer.class, name = "restrictAllTables"),
})
@UnstableApi
public interface PolicyEnforcer
{

  /**
   * Validate a data source against the policy enforcer.
   * @param ds data source to validate
   * @return true if the data source is valid according to the policy enforcer, false otherwise
   */
  boolean validate(DataSource ds);
}
