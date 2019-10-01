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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "NoopSupervisorSpec", value = NoopSupervisorSpec.class)
})
public interface SupervisorSpec
{
  /**
   * Return an unique id of {@link Supervisor}.
   */
  String getId();

  /**
   * Create a new {@link Supervisor} instance.
   */
  Supervisor createSupervisor();

  List<String> getDataSources();

  default SupervisorSpec createSuspendedSpec()
  {
    throw new UnsupportedOperationException();
  }

  default SupervisorSpec createRunningSpec()
  {
    throw new UnsupportedOperationException();
  }

  default boolean isSuspended()
  {
    return false;
  }

  /**
   * This API is only used for informational purposes in
   * org.apache.druid.sql.calcite.schema.SystemSchema.SupervisorsTable
   *
   * @return supervisor type
   */
  String getType();

  /**
   * This API is only used for informational purposes in
   * org.apache.druid.sql.calcite.schema.SystemSchema.SupervisorsTable
   *
   * @return source like stream or topic name
   */
  String getSource();
}
