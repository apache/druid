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
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.SupervisorTaskAutoScaler;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

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

  default SupervisorTaskAutoScaler createAutoscaler(Supervisor supervisor)
  {
    return null;
  }

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
   * @return The types of {@link org.apache.druid.data.input.InputSource} that the task uses. Empty set is returned if
   * the task does not use any. Users can be given permission to access particular types of
   * input sources but not others, using the
   * {@link org.apache.druid.server.security.AuthConfig#enableInputSourceSecurity} config.
   */
  default Set<String> getInputSourceTypes() {
    return ImmutableSet.of();
  }

  /**
   * @return Whether the task uses {@link org.apache.druid.data.input.Firehose}. If the
   * {@link org.apache.druid.server.security.AuthConfig#enableInputSourceSecurity} config is
   * enabled, then tasks that use firehose cannot be used.
   */
  default boolean usesFirehose() {
    return false;
  }

  /**
   * This API is only used for informational purposes in
   * org.apache.druid.sql.calcite.schema.SystemSchema.SupervisorsTable
   *
   * @return source like stream or topic name
   */
  String getSource();
}
