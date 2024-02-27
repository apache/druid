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

@JsonTypeName(NotEnoughTemporaryStorageFault.CODE)
public class NotEnoughTemporaryStorageFault extends BaseMSQFault
{
  static final String CODE = "NotEnoughTemporaryStorageFault";

  private final long suggestedMinimumStorage;
  private final long configuredTemporaryStorage;

  @JsonCreator
  public NotEnoughTemporaryStorageFault(
      @JsonProperty("suggestedMinimumStorage") final long suggestedMinimumStorage,
      @JsonProperty("configuredTemporaryStorage") final long configuredTemporaryStorage
  )
  {
    super(
        CODE,
        "Not enough temporary storage space for intermediate files. Requires at least %,d bytes. (configured = %,d bytes). Increase the limit by increasing tmpStorageBytesPerTask or "
        + "disable durable storage by setting the context parameter durableShuffleStorage as false.",
        suggestedMinimumStorage,
        configuredTemporaryStorage
    );

    this.suggestedMinimumStorage = suggestedMinimumStorage;
    this.configuredTemporaryStorage = configuredTemporaryStorage;
  }

  @JsonProperty
  public long getSuggestedMinimumStorage()
  {
    return suggestedMinimumStorage;
  }

  @JsonProperty
  public long getConfiguredTemporaryStorage()
  {
    return configuredTemporaryStorage;
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
    NotEnoughTemporaryStorageFault that = (NotEnoughTemporaryStorageFault) o;
    return suggestedMinimumStorage == that.suggestedMinimumStorage
           && configuredTemporaryStorage == that.configuredTemporaryStorage;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), suggestedMinimumStorage, configuredTemporaryStorage);
  }

  @Override
  public String toString()
  {
    return "NotEnoughTemporaryStorageFault{" +
           "suggestedMinimumStorage=" + suggestedMinimumStorage +
           ", configuredTemporaryStorage=" + configuredTemporaryStorage +
           '}';
  }
}
