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
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.SegmentReference;

/**
 * Interface for enforcing policies on data sources and segments in Druid queries.
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
   * Validates a {@link DataSource} against the policy enforcer. Prior to query execution, the {@link org.apache.druid.query.Query#getDataSource()}
   * tree is walked. This method is invoked once for each {@link org.apache.druid.query.RestrictedDataSource} and once
   * for each {@link TableDataSource} that is not wrapped inside a {@link org.apache.druid.query.RestrictedDataSource},
   * no matter where they appear within the tree.
   *
   * @param ds     the data source to validate. Always {@link TableDataSource}.
   * @param policy the policy attached to the table; either {@link org.apache.druid.query.RestrictedDataSource#policy} or null.
   * @throws DruidException if the data source does not comply with the policy
   */
  default void validateOrElseThrow(TableDataSource ds, Policy policy) throws DruidException
  {
    if (validate(policy)) {
      return;
    }
    throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                        .ofCategory(DruidException.Category.FORBIDDEN)
                        .build("Failed security validation with dataSource [%s]", ds);
  }

  /**
   * Validates a {@link SegmentReference} against the policy enforcer. This method is invoked during query execution via
   * {@link SegmentReference#validateOrElseThrow(PolicyEnforcer)}. The callsite must ensure that the segment reference
   * is not a deeply linked segment reference, and it must be referenced to a druid table.
   * <p>
   * Direct invocation of this method is discouraged; use {@link SegmentReference#validateOrElseThrow(PolicyEnforcer)} instead.
   *
   * @param segment the segment to validate
   * @param policy  the policy on the segment, {@link org.apache.druid.segment.RestrictedSegment#policy} or null for other
   * @throws DruidException if the segment does not comply with the policy
   */
  default void validateOrElseThrow(SegmentReference segment, Policy policy) throws DruidException
  {
    if (validate(policy)) {
      return;
    }
    throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                        .ofCategory(DruidException.Category.FORBIDDEN)
                        .build("Failed security validation with segment [%s]", segment.getId());
  }

  /**
   * Returns true if the policy complies with the policy enforcer.
   */
  boolean validate(Policy policy);
}
