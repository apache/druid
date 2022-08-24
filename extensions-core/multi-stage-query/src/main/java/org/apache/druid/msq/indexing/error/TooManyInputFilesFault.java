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
import org.apache.druid.msq.util.MultiStageQueryContext;

import java.util.Objects;

@JsonTypeName(TooManyInputFilesFault.CODE)
public class TooManyInputFilesFault extends BaseMSQFault
{
  static final String CODE = "TooManyInputFiles";

  private final int numInputFiles;
  private final int maxInputFiles;
  private final int minNumWorkers;


  @JsonCreator
  public TooManyInputFilesFault(
      @JsonProperty("numInputFiles") final int numInputFiles,
      @JsonProperty("maxInputFiles") final int maxInputFiles,
      @JsonProperty("minNumWorkers") final int minNumWorkers
  )
  {
    super(
        CODE,
        "Too many input files/segments [%d] encountered. Maximum input files/segments per worker is set to [%d]. Try"
        + " breaking your query up into smaller queries, or increasing the number of workers to at least [%d] by"
        + " setting %s in your query context",
        numInputFiles,
        maxInputFiles,
        minNumWorkers,
        MultiStageQueryContext.CTX_MAX_NUM_TASKS
    );
    this.numInputFiles = numInputFiles;
    this.maxInputFiles = maxInputFiles;
    this.minNumWorkers = minNumWorkers;
  }

  @JsonProperty
  public int getNumInputFiles()
  {
    return numInputFiles;
  }

  @JsonProperty
  public int getMaxInputFiles()
  {
    return maxInputFiles;
  }

  @JsonProperty
  public int getMinNumWorkers()
  {
    return minNumWorkers;
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
    TooManyInputFilesFault that = (TooManyInputFilesFault) o;
    return numInputFiles == that.numInputFiles
           && maxInputFiles == that.maxInputFiles
           && minNumWorkers == that.minNumWorkers;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), numInputFiles, maxInputFiles, minNumWorkers);
  }
}
