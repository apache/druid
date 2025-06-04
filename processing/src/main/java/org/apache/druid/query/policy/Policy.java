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
import org.apache.druid.segment.CursorBuildSpec;

/**
 * Extensible interface for a granular-level (e.x. row filter) restriction on read-table access. Implementations must be
 * Jackson-serializable.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = RowFilterPolicy.class, name = "row"),
    @JsonSubTypes.Type(value = NoRestrictionPolicy.class, name = "noRestriction")
})
@UnstableApi
public interface Policy
{
  /**
   * Apply this policy to a {@link CursorBuildSpec} to seamlessly enforce policies for cursor-based queries. The
   * application must encapsulate 100% of the requirements of this policy.
   * <p>
   * Any transforms done to the spec must be sure to update {@link CursorBuildSpec#getPhysicalColumns()} and
   * {@link CursorBuildSpec#getVirtualColumns()} as needed to ensure the underlying
   * {@link org.apache.druid.segment.CursorHolder} that is being restricted has accurate information about the set of
   * required columns.
   */
  CursorBuildSpec visit(CursorBuildSpec spec);

}
