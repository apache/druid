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

package org.apache.druid.testing.utils;

import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskIOConfig;
import org.apache.druid.server.coordinator.UserCompactionTaskQueryTuningConfig;
import org.joda.time.Period;

/**
 * Contains utility methods for Compaction.
 */
public class CompactionUtil
{

  private CompactionUtil()
  {
    // no instantiation
  }

  public static DataSourceCompactionConfig createCompactionConfig(
      String fullDatasourceName,
      Integer maxRowsPerSegment,
      Period skipOffsetFromLatest
  )
  {
    return new DataSourceCompactionConfig(
        fullDatasourceName,
        null,
        null,
        null,
        skipOffsetFromLatest,
        new UserCompactionTaskQueryTuningConfig(
            null,
            null,
            null,
            new MaxSizeSplitHintSpec(null, 1),
            new DynamicPartitionsSpec(maxRowsPerSegment, null),
            null,
            null,
            null,
            null,
            null,
            1,
            null,
            null,
            null,
            null,
            null,
            1
        ),
        null,
        new UserCompactionTaskIOConfig(true),
        null
    );
  }

}
