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
import org.apache.druid.segment.IndexSpec;
import org.joda.time.Period;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Objects;

/**
 * A {@link ReindexingRule} that specifies an {@link IndexSpec} for reindexing tasks to configure.
 * <p>
 * This rule controls compression and encoding settings independently from partitioning.
 * For example, applying different bitmap or metric compression to older data.
 * <p>
 * This is a non-additive rule. Multiple index spec rules cannot be applied to the same interval.
 * <p>
 * Example inline usage:
 * <pre>{@code
 * {
 *   "id": "compressed-90d",
 *   "olderThan": "P90D",
 *   "indexSpec": {
 *     "bitmap": { "type": "roaring" },
 *     "metricCompression": "lz4"
 *   },
 *   "description": "Use roaring bitmaps and lz4 for data older than 90 days"
 * }
 * }</pre>
 */
public class ReindexingIndexSpecRule extends AbstractReindexingRule
{
  private final IndexSpec indexSpec;

  @JsonCreator
  public ReindexingIndexSpecRule(
      @JsonProperty("id") @Nonnull String id,
      @JsonProperty("description") @Nullable String description,
      @JsonProperty("olderThan") @Nonnull Period olderThan,
      @JsonProperty("indexSpec") @Nonnull IndexSpec indexSpec
  )
  {
    super(id, description, olderThan);
    InvalidInput.conditionalException(indexSpec != null, "'indexSpec' cannot be null");
    this.indexSpec = indexSpec;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
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
    ReindexingIndexSpecRule that = (ReindexingIndexSpecRule) o;
    return Objects.equals(getId(), that.getId())
           && Objects.equals(getDescription(), that.getDescription())
           && Objects.equals(getOlderThan(), that.getOlderThan())
           && Objects.equals(indexSpec, that.indexSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getDescription(),
        getOlderThan(),
        indexSpec
    );
  }

  @Override
  public String toString()
  {
    return "ReindexingIndexSpecRule{"
           + "id='" + getId() + '\''
           + ", description='" + getDescription() + '\''
           + ", olderThan=" + getOlderThan()
           + ", indexSpec=" + indexSpec
           + '}';
  }
}
