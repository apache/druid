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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SegmentsTableConfig
{
  @JsonProperty
  private boolean forceHashBasedMerge = false;

  /**
   * This controls the max size of {@link org.apache.druid.sql.calcite.schema.ObjectStringCache},
   * which is used to cache the string representation of Java objects
   * such as {@link org.joda.time.DateTime#toString()} or
   * serialized JSON of {@link org.apache.druid.timeline.CompactionState}.
   * Since these objects are usually expected to have a lot of duplicates when processing the segments table,
   * the default size should be enough in most cases. For example, the cache will contain only 26,280 entries
   * to store the timestamp of segment intervals when you have hourly partitioned segments stored for 3 years.
   *
   * @see org.apache.druid.sql.calcite.schema.SegmentsTableRow
   */
  @JsonProperty
  private long stringCacheSizeRows = 100_000;

  public boolean isForceHashBasedMerge()
  {
    return forceHashBasedMerge;
  }

  public long getStringCacheSizeRows()
  {
    return stringCacheSizeRows;
  }
}
