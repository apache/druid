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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName(NotEnoughMemoryFault.CODE)
public class NotEnoughMemoryFault extends BaseMSQFault
{
  static final String CODE = "NotEnoughMemory";

  private final long serverMemory;
  private final int serverWorkers;
  private final int serverThreads;

  @JsonCreator
  public NotEnoughMemoryFault(
      @JsonProperty("serverMemory") final long serverMemory,
      @JsonProperty("serverWorkers") final int serverWorkers,
      @JsonProperty("serverThreads") final int serverThreads
  )
  {
    super(
        CODE,
        "Not enough memory (available = %,d; server workers = %,d; server threads = %,d)",
        serverMemory,
        serverWorkers,
        serverThreads
    );

    this.serverMemory = serverMemory;
    this.serverWorkers = serverWorkers;
    this.serverThreads = serverThreads;
  }

  @JsonProperty
  public long getServerMemory()
  {
    return serverMemory;
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
    return serverMemory == that.serverMemory
           && serverWorkers == that.serverWorkers
           && serverThreads == that.serverThreads;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), serverMemory, serverWorkers, serverThreads);
  }

  @Override
  public String toString()
  {
    return "NotEnoughMemoryFault{" +
           "serverMemory=" + serverMemory +
           ", serverWorkers=" + serverWorkers +
           ", serverThreads=" + serverThreads +
           '}';
  }
}
