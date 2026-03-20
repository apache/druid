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
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies a {@link TuningConfig} for tasks to configure.
 * <p>
 * This rule controls things like partitioning strategy. For example, applying range partitioning over specific
 * dimensions to older data can optimize query performance for common access patterns.
 * <p>
 * This is a non-additive rule. Multiple tuning config rules cannot be applied to the same interval, as a compaction
 * job can only use one tuning configuration.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *   "id": "range-partition-30d",
 *   "olderThan": "P30D",
 *   "tuningConfig": {
 *     "partitionsSpec": {
 *       "type": "range",
 *       "targetRowsPerSegment": 5000000,
 *       "partitionDimensions": ["country", "city"]
 *     }
 *   },
 *   "description": "Use range partitioning for data older than 30 days"
 * }
 * }</pre>
 */
public class ReindexingTuningConfigRule extends AbstractReindexingRule
{
  private final UserCompactionTaskQueryTuningConfig tuningConfig;

  @JsonCreator
  public ReindexingTuningConfigRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("tuningConfig") @Nonnull UserCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    super(id, description, olderThan);
    InvalidInput.conditionalException(tuningConfig != null, "'tuningConfig' cannot be null");
    this.tuningConfig = tuningConfig;
  }

  @JsonProperty
  public UserCompactionTaskQueryTuningConfig getTuningConfig()
  {
    return tuningConfig;
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
    ReindexingTuningConfigRule that = (ReindexingTuningConfigRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(tuningConfig, that.tuningConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        tuningConfig
    );
  }

  @Override
  public String toString()
  {
    return "ReindexingTuningConfigRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", tuningConfig=" + tuningConfig
           + '}';
  }
}
