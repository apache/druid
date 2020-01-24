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

package org.apache.druid.indextable.loader.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.common.task.IndexTask;

import java.util.List;

/**
 * The config required for an indexed table to be loaded from an ingestion spec.
 */
public class IndexedTableConfig
{
  /**
   * The ingestion spec needed to load the indexed table
   */
  private final IndexTask.IndexIngestionSpec ingestionSpec;

  /**
   * The keys on this indexed table.
   */
  private final List<String> keys;

  @JsonCreator
  public IndexedTableConfig(
      @JsonProperty("ingestionSpec") IndexTask.IndexIngestionSpec ingestionSpec,
      @JsonProperty("keys") List<String> keys
  )
  {
    this.ingestionSpec = ingestionSpec;
    this.keys = keys;
  }

  public IndexTask.IndexIngestionSpec getIngestionSpec()
  {
    return ingestionSpec;
  }

  public List<String> getKeys()
  {
    return keys;
  }
}
