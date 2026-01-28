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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Objects;

@JsonTypeName(NotEnoughMemoryFault.CODE)
public class NotEnoughMemoryFault extends BaseMSQFault
{
  static final String CODE = "NotEnoughMemory";

  private final long suggestedServerMemory;
  private final long serverMemory;
  private final long usableMemory;
  private final int serverWorkers;
  private final int serverThreads;
  private final int inputWorkers;
  private final int maxConcurrentStages;

  @JsonCreator
  public NotEnoughMemoryFault(
      @JsonProperty("suggestedServerMemory") final long suggestedServerMemory,
      @JsonProperty("serverMemory") final long serverMemory,
      @JsonProperty("usableMemory") final long usableMemory,
      @JsonProperty("serverWorkers") final int serverWorkers,
      @JsonProperty("serverThreads") final int serverThreads,
      @JsonProperty("inputWorkers") final int inputWorkers,
      @JsonProperty("maxConcurrentStages") final int maxConcurrentStages
  )
  {
    super(
        CODE,
        "Not enough memory. "
        + (suggestedServerMemory > 0
           ? StringUtils.format("Minimum bytes[%,d] is needed for the current configuration. ", suggestedServerMemory)
           : "")
        + "(total bytes[%,d]; "
        + "usable bytes[%,d]; "
        + "input workers[%,d]; "
        + "concurrent stages[%,d]; "
        + "server worker capacity[%,d]; "
        + "server processing threads[%,d]). "
        + "Increase JVM memory with the -Xmx option"
        + (inputWorkers > 1 ? ", or reduce maxNumTasks for this query" : "")
        + (maxConcurrentStages > 1 ? ", or reduce maxConcurrentStages for this query" : "")
        + (serverWorkers > 1 ? ", or reduce worker capacity on this server" : "")
        + (serverThreads > 1 ? ", or reduce processing threads on this server" : ""),
        serverMemory,
        usableMemory,
        inputWorkers,
        maxConcurrentStages,
        serverWorkers,
        serverThreads
    );

    this.suggestedServerMemory = suggestedServerMemory;
    this.serverMemory = serverMemory;
    this.usableMemory = usableMemory;
    this.serverWorkers = serverWorkers;
    this.serverThreads = serverThreads;
    this.inputWorkers = inputWorkers;
    this.maxConcurrentStages = maxConcurrentStages;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public long getSuggestedServerMemory()
  {
    return suggestedServerMemory;
  }

  @JsonProperty
  public long getServerMemory()
  {
    return serverMemory;
  }

  @JsonProperty
  public long getUsableMemory()
  {
    return usableMemory;
  }

  @JsonProperty
  public int getServerWorkers()
  {
    return serverWorkers;
  }

  @JsonProperty
  public int getServerThreads()
  {
    return serverThreads;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getInputWorkers()
  {
    return inputWorkers;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getMaxConcurrentStages()
  {
    return maxConcurrentStages;
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
    if (!super.equals(o)) {
      return false;
    }
    NotEnoughMemoryFault that = (NotEnoughMemoryFault) o;
    return suggestedServerMemory == that.suggestedServerMemory
           && serverMemory == that.serverMemory
           && usableMemory == that.usableMemory
           && serverWorkers == that.serverWorkers
           && serverThreads == that.serverThreads
           && inputWorkers == that.inputWorkers
           && maxConcurrentStages == that.maxConcurrentStages;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        suggestedServerMemory,
        serverMemory,
        usableMemory,
        serverWorkers,
        serverThreads,
        inputWorkers,
        maxConcurrentStages
    );
  }

  @Override
  public String toString()
  {
    return "NotEnoughMemoryFault{" +
           "suggestedServerMemory=" + suggestedServerMemory +
           ", serverMemory=" + serverMemory +
           ", usableMemory=" + usableMemory +
           ", serverWorkers=" + serverWorkers +
           ", serverThreads=" + serverThreads +
           ", inputWorkers=" + inputWorkers +
           ", maxConcurrentStages=" + maxConcurrentStages +
           '}';
  }
}
