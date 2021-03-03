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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "realtime", value = RealtimeTuningConfig.class)
})
public interface TuningConfig
{
  boolean DEFAULT_LOG_PARSE_EXCEPTIONS = false;
  AppendableIndexSpec DEFAULT_APPENDABLE_INDEX = new OnheapIncrementalIndex.Spec();
  int DEFAULT_MAX_PARSE_EXCEPTIONS = Integer.MAX_VALUE;
  int DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS = 0;
  int DEFAULT_MAX_ROWS_IN_MEMORY = 1_000_000;
  boolean DEFAULT_SKIP_BYTES_IN_MEMORY_OVERHEAD_CHECK = false;

  /**
   * The incremental index implementation to use
   */
  AppendableIndexSpec getAppendableIndexSpec();

  /**
   * Maximum number of rows in memory before persisting to local storage
   */
  int getMaxRowsInMemory();

  /**
   * Maximum number of bytes (estimated) to store in memory before persisting to local storage
   */
  long getMaxBytesInMemory();

  /**
   * Maximum number of bytes (estimated) to store in memory before persisting to local storage.
   * If getMaxBytesInMemory() returns 0, the appendable index default will be used.
   */
  default long getMaxBytesInMemoryOrDefault()
  {
    // In the main tuningConfig class constructor, we set the maxBytes to 0 if null to avoid setting
    // maxBytes to max jvm memory of the process that starts first. Instead we set the default based on
    // the actual task node's jvm memory.
    final long maxBytesInMemory = getMaxBytesInMemory();
    if (maxBytesInMemory > 0) {
      return maxBytesInMemory;
    } else if (maxBytesInMemory == 0) {
      return getAppendableIndexSpec().getDefaultMaxBytesInMemory();
    } else {
      return Long.MAX_VALUE;
    }
  }

  PartitionsSpec getPartitionsSpec();

  IndexSpec getIndexSpec();

  IndexSpec getIndexSpecForIntermediatePersists();
}
