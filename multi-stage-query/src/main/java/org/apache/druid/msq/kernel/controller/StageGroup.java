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

package org.apache.druid.msq.kernel.controller;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.msq.exec.OutputChannelMode;
import org.apache.druid.msq.kernel.StageId;

import java.util.List;
import java.util.Objects;

/**
 * Group of stages that must be launched as a unit. Within each group, stages communicate with each other using
 * {@link OutputChannelMode#MEMORY} channels. The final stage in a group writes its own output using
 * {@link #lastStageOutputChannelMode()}.
 *
 * Stages in a group have linear (non-branching) data flow: the first stage is an input to the second stage, the second
 * stage is an input to the third stage, and so on. This is done to simplify the logic. In the future, it is possible
 * that stage groups may contain branching data flow.
 */
public class StageGroup
{
  private final List<StageId> stageIds;
  private final OutputChannelMode groupOutputChannelMode;

  public StageGroup(final List<StageId> stageIds, final OutputChannelMode groupOutputChannelMode)
  {
    this.stageIds = stageIds;
    this.groupOutputChannelMode = groupOutputChannelMode;
  }

  /**
   * List of stage IDs in this group.
   *
   * The first stage is an input to the second stage, the second stage is an input to the third stage, and so on.
   * See class-level javadocs for more details.
   */
  public List<StageId> stageIds()
  {
    return stageIds;
  }

  /**
   * Output mode of the final stage in this group.
   */
  public OutputChannelMode lastStageOutputChannelMode()
  {
    return stageOutputChannelMode(last());
  }

  /**
   * Output mode of the given stage.
   */
  public OutputChannelMode stageOutputChannelMode(final StageId stageId)
  {
    if (last().equals(stageId)) {
      return groupOutputChannelMode;
    } else if (stageIds.contains(stageId)) {
      return OutputChannelMode.MEMORY;
    } else {
      throw new IAE("Stage[%s] not in group", stageId);
    }
  }

  /**
   * First stage in this group.
   */
  public StageId first()
  {
    return stageIds.get(0);
  }

  /**
   * Last stage in this group.
   */
  public StageId last()
  {
    return stageIds.get(stageIds.size() - 1);
  }

  /**
   * Number of stages in this group.
   */
  public int size()
  {
    return stageIds.size();
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
    StageGroup that = (StageGroup) o;
    return Objects.equals(stageIds, that.stageIds) && groupOutputChannelMode == that.groupOutputChannelMode;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(stageIds, groupOutputChannelMode);
  }

  @Override
  public String toString()
  {
    return "StageGroup{" +
           "stageIds=" + stageIds +
           ", groupOutputChannelMode=" + groupOutputChannelMode +
           '}';
  }
}
