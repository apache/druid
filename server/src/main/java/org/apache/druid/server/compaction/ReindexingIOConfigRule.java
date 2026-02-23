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

package org.apache.druid.server.compaction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies a {@link UserCompactionTaskIOConfig} for tasks to configure.
 * <p>
 * This is a non-additive rule. Multiple IO config rules cannot be applied to the same interval safely,
 * as a compaction job can only use one IO configuration.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *   "id": "dropExistingFalse-false",
 *   "olderThan": "P90D",
 *   "ioConfig": {
 *     "dropExisting": false
 *   },
 * }
 * }</pre>
 */
public class ReindexingIOConfigRule extends AbstractReindexingRule
{
  private final UserCompactionTaskIOConfig ioConfig;

  @JsonCreator
  public ReindexingIOConfigRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("ioConfig") @Nonnull UserCompactionTaskIOConfig ioConfig
  )
  {
    super(id, description, olderThan);
    InvalidInput.conditionalException(ioConfig != null, "'ioConfig' cannot be null");
    this.ioConfig = ioConfig;
  }

  @JsonProperty
  public UserCompactionTaskIOConfig getIoConfig()
  {
    return ioConfig;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ReindexingIOConfigRule that = (ReindexingIOConfigRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(ioConfig, that.ioConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        ioConfig
    );
  }

  @Override
  public String toString()
  {
    return "ReindexingIOConfigRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", ioConfig=" + ioConfig
           + '}';
  }
}
