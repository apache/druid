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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;

import java.util.Map;

/**
 * Configuration for bounded (one-time) stream processing with explicit start/end offsets.
 *
 * When configured, the supervisor will:
 * 1. Create tasks starting at the specified startSequenceNumbers
 * 2. Tasks will automatically stop when they reach endSequenceNumbers
 * 3. Supervisor will not recreate tasks after they complete
 * 4. Supervisor will auto-terminate when all tasks are done
 *
 * This is useful for:
 * - Backfill processing
 * - Historical reprocessing
 * - One-time migration tasks
 */
public class BoundedStreamConfig
{
  private final Map<?, ?> startSequenceNumbers;  // Partition -> Start Offset
  private final Map<?, ?> endSequenceNumbers;    // Partition -> End Offset

  @JsonCreator
  public BoundedStreamConfig(
      @JsonProperty("startSequenceNumbers") Map<?, ?> startSequenceNumbers,
      @JsonProperty("endSequenceNumbers") Map<?, ?> endSequenceNumbers
  )
  {
    if (startSequenceNumbers == null || startSequenceNumbers.isEmpty() ||
        endSequenceNumbers == null || endSequenceNumbers.isEmpty()) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("startSequenceNumbers and endSequenceNumbers cannot be null or empty");
    }

    if (!startSequenceNumbers.keySet().equals(endSequenceNumbers.keySet())) {
      throw DruidException.forPersona(DruidException.Persona.ADMIN)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "startSequenceNumbers and endSequenceNumbers must have matching partition sets. Start: %s, End: %s",
                              startSequenceNumbers.keySet(),
                              endSequenceNumbers.keySet()
                          );
    }

    this.startSequenceNumbers = startSequenceNumbers;
    this.endSequenceNumbers = endSequenceNumbers;
  }

  @JsonProperty
  public Map<?, ?> getStartSequenceNumbers()
  {
    return startSequenceNumbers;
  }

  @JsonProperty
  public Map<?, ?> getEndSequenceNumbers()
  {
    return endSequenceNumbers;
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
    BoundedStreamConfig that = (BoundedStreamConfig) o;
    return startSequenceNumbers.equals(that.startSequenceNumbers) &&
           endSequenceNumbers.equals(that.endSequenceNumbers);
  }

  @Override
  public int hashCode()
  {
    int result = startSequenceNumbers.hashCode();
    result = 31 * result + endSequenceNumbers.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "BoundedStreamConfig{" +
           "startSequenceNumbers=" + startSequenceNumbers +
           ", endSequenceNumbers=" + endSequenceNumbers +
           '}';
  }
}
