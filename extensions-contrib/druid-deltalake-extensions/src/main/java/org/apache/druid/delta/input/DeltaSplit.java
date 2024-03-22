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

package org.apache.druid.delta.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * An input split of a Delta table containing the following information:
 * <li>
 * {@code stateRow} represents the canonical json representation of the latest snapshot of the Delta table.
 * </li>
 * <li>
 * {@code files} represents the list of files from the latest snapshot.
 * </li>
 */
public class DeltaSplit
{
  private final String stateRow;
  private final List<String> files;

  @JsonCreator
  public DeltaSplit(
      @JsonProperty("state") final String stateRow,
      @JsonProperty("files") final List<String> files
  )
  {
    this.stateRow = stateRow;
    this.files = files;
  }

  @JsonProperty("state")
  public String getStateRow()
  {
    return stateRow;
  }

  @JsonProperty("files")
  public List<String> getFiles()
  {
    return files;
  }

  @Override
  public String toString()
  {
    return "DeltaSplit{" +
           "stateRow=" + stateRow +
           ", files=" + files +
           "}";
  }
}
