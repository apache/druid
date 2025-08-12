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

package org.apache.druid.msq.kernel;

import org.apache.druid.java.util.common.StringUtils;

import java.util.Comparator;
import java.util.Objects;

/**
 * Represents a particular output partition of a particular stage.
 */
public class StagePartition implements Comparable<StagePartition>
{
  // StagePartition is Comparable because it is used as a sorted map key in InputChannels.
  private static final Comparator<StagePartition> COMPARATOR =
      Comparator.comparing(StagePartition::getStageId)
                .thenComparing(StagePartition::getPartitionNumber);

  private final StageId stageId;
  private final int partitionNumber;

  public StagePartition(StageId stageId, int partitionNumber)
  {
    this.stageId = stageId;
    this.partitionNumber = partitionNumber;
  }

  public StageId getStageId()
  {
    return stageId;
  }

  public int getPartitionNumber()
  {
    return partitionNumber;
  }

  @Override
  public int compareTo(final StagePartition that)
  {
    return COMPARATOR.compare(this, that);
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
    StagePartition that = (StagePartition) o;
    return partitionNumber == that.partitionNumber && Objects.equals(stageId, that.stageId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageId, partitionNumber);
  }

  @Override
  public String toString()
  {
    return StringUtils.format("%s_%s", stageId, partitionNumber);
  }
}
