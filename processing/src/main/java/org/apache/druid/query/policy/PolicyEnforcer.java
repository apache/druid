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
 * <p>
 * Note: The {@code PolicyEnforcer} is intended to serve as a sanity checker and not as a primary authorization mechanism.
 * It should not be used to implement security rules. Instead, it acts as a last line of defense to verify that
 * security policies have been implemented correctly and to prevent incorrect policy usage.
 * </p>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = NoopPolicyEnforcer.class, name = "none"),
    @JsonSubTypes.Type(value = RestrictAllTablesPolicyEnforcer.class, name = "restrictAllTables"),
})
@UnstableApi
public interface PolicyEnforcer
{

  /**
   * Validate a {@link DataSource} against the policy enforcer. The data source must be authorized and include an
   * attached {@link Policy}, if applicable. The enforcer will validate it before executing any queries on the data
   * source.
   *
   * @param ds data source to validate
   * @return true if the data source is valid according to the policy enforcer, false otherwise
   */
  boolean validate(DataSource ds);
}
