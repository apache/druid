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
import org.apache.druid.segment.Segment;

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
   * Validate a {@link DataSource} node against the policy enforcer. This method is called during query planning from
   * {@link DataSource#validate(PolicyEnforcer)}.
   * <p>
   * Don't call this method directly, use {@link DataSource#validate(PolicyEnforcer)}.
   *
   * @param ds data source to validate
   * @return true if the data source is valid according to the policy enforcer, false otherwise
   */
  boolean validate(DataSource ds, Policy policy);


  /**
   * Validate a {@link Segment} node against the policy enforcer. This method is called during query planning from
   * {@link Segment#validate(PolicyEnforcer)}.
   * <p>
   * Don't call this method directly, use {@link Segment#validate(PolicyEnforcer)}.
   *
   * @param segment segment to validate
   * @return true if the segment is valid according to the policy enforcer, false otherwise
   */
  boolean validate(Segment segment, Policy policy);
}
