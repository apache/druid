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

package org.apache.druid.tests.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.clients.OverlordResourceTestClient;
import org.apache.druid.testing.utils.RetryUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

public abstract class AbstractIndexerTest
{

  @Inject
  protected CoordinatorResourceTestClient coordinator;
  @Inject
  protected OverlordResourceTestClient indexer;
  @Inject
  @Json
  protected ObjectMapper jsonMapper;
  @Inject
  @Smile
  protected ObjectMapper smileMapper;
  @Inject
  protected TestQueryHelper queryHelper;

  @Inject
  private IntegrationTestingConfig config;

  protected Closeable unloader(final String dataSource)
  {
    return () -> unloadAndKillData(dataSource);
  }

  protected void unloadAndKillData(final String dataSource)
  {
    List<String> intervals = coordinator.getSegmentIntervals(dataSource);

    // each element in intervals has this form:
    //   2015-12-01T23:15:00.000Z/2015-12-01T23:16:00.000Z
    // we'll sort the list (ISO dates have lexicographic order)
    // then delete segments from the 1st date in the first string
    // to the 2nd date in the last string
    Collections.sort(intervals);
    String first = intervals.get(0).split("/")[0];
    String last = intervals.get(intervals.size() - 1).split("/")[1];
    unloadAndKillData(dataSource, first, last);
  }

  private void unloadAndKillData(final String dataSource, String start, String end)
  {
    // Wait for any existing index tasks to complete before disabling the datasource otherwise
    // realtime tasks can get stuck waiting for handoff. https://github.com/apache/incubator-druid/issues/1729
    waitForAllTasksToComplete();
    Interval interval = Intervals.of(start + "/" + end);
    coordinator.unloadSegmentsForDataSource(dataSource);
    RetryUtil.retryUntilFalse(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call()
          {
            return coordinator.areSegmentsLoaded(dataSource);
          }
        }, "Segment Unloading"
    );
    coordinator.deleteSegmentsDataSource(dataSource, interval);
    waitForAllTasksToComplete();
  }

  protected void waitForAllTasksToComplete()
  {
    RetryUtil.retryUntilTrue(
        () -> {
          int numTasks = indexer.getPendingTasks().size() +
                         indexer.getRunningTasks().size() +
                         indexer.getWaitingTasks().size();
          return numTasks == 0;
        },
        "Waiting for Tasks Completion"
    );
  }

  protected String getTaskAsString(String file) throws IOException
  {
    final InputStream inputStream = ITRealtimeIndexTaskTest.class.getResourceAsStream(file);
    try {
      return IOUtils.toString(inputStream, "UTF-8");
    }
    finally {
      IOUtils.closeQuietly(inputStream);
    }
  }

}
