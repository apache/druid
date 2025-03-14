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

package org.apache.druid.server.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.error.DruidException;

public class HistoricalLoadingCapabilities
{
  private final int numLoadingThreads;
  private final int numTurboLoadingThreads;

  @JsonCreator
  public HistoricalLoadingCapabilities(
      @JsonProperty("numLoadingThreads") int numLoadingThreads,
      @JsonProperty("numTurboLoadingThreads") int numTurboLoadingThreads
  )
  {
    if (numLoadingThreads < 1 || numTurboLoadingThreads < 1) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("numLoadingThreads and numTurboLoadingThreads must be greater than 0.");
    }
    this.numLoadingThreads = numLoadingThreads;
    this.numTurboLoadingThreads = numTurboLoadingThreads;
  }

  @JsonProperty("numLoadingThreads")
  public int getNumLoadingThreads()
  {
    return numLoadingThreads;
  }

  @JsonProperty("numTurboLoadingThreads")
  public int getNumTurboLoadingThreads()
  {
    return numTurboLoadingThreads;
  }
}
