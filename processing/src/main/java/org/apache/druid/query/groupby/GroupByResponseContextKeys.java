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
 * software distributed under this License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.groupby;

import org.apache.druid.query.context.ResponseContext;

/**
 * Response context keys for GroupBy query metrics.
 * These keys are used to aggregate metrics from parallel query execution threads
 * using MAX aggregation (taking the maximum value across all threads).
 */
public class GroupByResponseContextKeys
{
  public static final String GROUPBY_MERGE_BUFFER_ACQUISITION_TIME_NAME = "groupByMergeBufferAcquisitionTime";
  public static final String GROUPBY_BYTES_SPILLED_TO_STORAGE_NAME = "groupByBytesSpilledToStorage";
  public static final String GROUPBY_MERGE_DICTIONARY_SIZE_NAME = "groupByMergeDictionarySize";

  /**
   * Maximum bytes spilled to storage across all parallel threads processing segments.
   * This represents the peak disk usage during query execution.
   */
  public static final ResponseContext.Key GROUPBY_BYTES_SPILLED_TO_STORAGE_KEY =
      new ResponseContext.LongKey(GROUPBY_BYTES_SPILLED_TO_STORAGE_NAME, false)
  {
    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return Math.max((Long) oldValue, (Long) newValue);
    }
  };

  /**
   * Maximum merge dictionary size across all parallel threads processing segments.
   * This represents the peak dictionary size used during query execution.
   */
  public static final ResponseContext.Key GROUPBY_MERGE_DICTIONARY_SIZE_KEY =
      new ResponseContext.LongKey(GROUPBY_MERGE_DICTIONARY_SIZE_NAME, false)
  {
    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return Math.max((Long) oldValue, (Long) newValue);
    }
  };

  /**
   * Maximum merge buffer acquisition time across all parallel threads processing segments.
   * This represents the longest time any thread waited to acquire a merge buffer.
   */
  public static final ResponseContext.Key GROUPBY_MERGE_BUFFER_ACQUISITION_TIME_KEY =
      new ResponseContext.LongKey(GROUPBY_MERGE_BUFFER_ACQUISITION_TIME_NAME, false)
  {
    @Override
    public Object mergeValues(Object oldValue, Object newValue)
    {
      return Math.max((Long) oldValue, (Long) newValue);
    }
  };

  static {
    ResponseContext.Keys.instance().registerKeys(
        new ResponseContext.Key[]{
            GROUPBY_BYTES_SPILLED_TO_STORAGE_KEY,
            GROUPBY_MERGE_DICTIONARY_SIZE_KEY,
            GROUPBY_MERGE_BUFFER_ACQUISITION_TIME_KEY
        }
    );
  }
}
