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

package org.apache.druid.segment.realtime.appenderator;

import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

public interface AppenderatorConfig extends TuningConfig
{

  int DEFAULT_NUM_PERSIST_THREADS = 1;

  boolean isReportParseExceptions();

  int getMaxPendingPersists();

  boolean isSkipBytesInMemoryOverheadCheck();

  default int getNumPersistThreads()
  {
    return DEFAULT_NUM_PERSIST_THREADS;
  }

  /**
   * Maximum number of rows in a single segment before pushing to deep storage
   */
  @Nullable
  default Integer getMaxRowsPerSegment()
  {
    return Integer.MAX_VALUE;
  }

  /**
   * Maximum number of rows across all segments before pushing to deep storage
   */
  @Nullable
  default Long getMaxTotalRows()
  {
    throw new UnsupportedOperationException("maxTotalRows is not implemented.");
  }

  /**
   * Period that sets frequency to persist to local storage if no other thresholds are met
   */
  Period getIntermediatePersistPeriod();

  File getBasePersistDirectory();

  AppenderatorConfig withBasePersistDirectory(File basePersistDirectory);

  @Nullable
  SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory();

  default int getMaxColumnsToMerge()
  {
    return -1;
  }
}
