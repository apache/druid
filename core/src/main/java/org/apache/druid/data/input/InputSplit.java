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

package org.apache.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Input unit for distributed batch ingestion. Used in {@link FiniteFirehoseFactory}.
 * An {@link InputSplit} represents the input data processed by a {@code org.apache.druid.indexing.common.task.Task}.
 */
public class InputSplit<T>
{
  private final T split;

  @JsonCreator
  public InputSplit(@JsonProperty("split") T split)
  {
    this.split = split;
  }

  @JsonProperty("split")
  public T get()
  {
    return split;
  }

  @Override
  public String toString()
  {
    return "InputSplit{" +
           "split=" + split +
           "}";
  }
}
