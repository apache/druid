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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.kernel.controller.ControllerQueryKernelUtils;

/**
 * Mode for stage output channels. Provided to workers in {@link WorkOrder#getOutputChannelMode()}.
 */
public enum OutputChannelMode
{
  /**
   * In-memory output channels. Stage shuffle data does not hit disk. This mode requires a consumer stage to run
   * at the same time as its corresponding producer stage. See {@link ControllerQueryKernelUtils#computeStageGroups} for the
   * logic that determines when we can use in-memory channels.
   */
  MEMORY("memory"),

  /**
   * Local file output channels. Stage shuffle data is stored in files on disk on the producer, and served via HTTP
   * to the consumer.
   */
  LOCAL_STORAGE("localStorage"),

  /**
   * Durable storage output channels. Stage shuffle data is written by producers to durable storage (e.g. cloud
   * storage), and is read from durable storage by consumers.
   */
  DURABLE_STORAGE_INTERMEDIATE("durableStorage"),

  /**
   * Like {@link #DURABLE_STORAGE_INTERMEDIATE}, but a special case for the final stage
   * {@link QueryDefinition#getFinalStageDefinition()}. The structure of files in deep storage is somewhat different.
   */
  DURABLE_STORAGE_QUERY_RESULTS("durableStorageQueryResults");

  private final String name;

  OutputChannelMode(String name)
  {
    this.name = name;
  }

  @JsonCreator
  public static OutputChannelMode fromString(final String s)
  {
    for (final OutputChannelMode mode : values()) {
      if (mode.toString().equals(s)) {
        return mode;
      }
    }

    throw new IAE("No such outputChannelMode[%s]", s);
  }

  /**
   * Whether this mode involves writing to durable storage.
   */
  public boolean isDurable()
  {
    return this == DURABLE_STORAGE_INTERMEDIATE || this == DURABLE_STORAGE_QUERY_RESULTS;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return name;
  }
}
