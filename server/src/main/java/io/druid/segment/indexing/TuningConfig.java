/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "realtime", value = RealtimeTuningConfig.class)
})
public interface TuningConfig
{
  boolean DEFAULT_LOG_PARSE_EXCEPTIONS = false;
  int DEFAULT_MAX_PARSE_EXCEPTIONS = Integer.MAX_VALUE;
  int DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS = 0;
  int DEFAULT_MAX_ROWS_IN_MEMORY = 1_000_000;
  // We initially estimated this to be 1/3(max jvm memory), but bytesCurrentlyInMemory only
  // tracks active index and not the index being flushed to disk, to account for that
  // we halved default to 1/6(max jvm memory)
  long DEFAULT_MAX_BYTES_IN_MEMORY = Runtime.getRuntime().maxMemory() / 6;
}
