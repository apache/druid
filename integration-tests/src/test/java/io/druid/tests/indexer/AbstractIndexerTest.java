/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.tests.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.testing.clients.CoordinatorResourceTestClient;
import io.druid.testing.clients.OverlordResourceTestClient;
import io.druid.testing.utils.RetryUtil;
import io.druid.testing.utils.TestQueryHelper;
import org.apache.commons.io.IOUtils;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.InputStream;
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

  protected void unloadAndKillData(final String dataSource) throws Exception
  {
      unloadAndKillData (dataSource, "2013-01-01T00:00:00.000Z", "2013-12-01T00:00:00.000Z");
  }

  protected void unloadAndKillData(final String dataSource, String start, String end) throws Exception
  {
    Interval interval = new Interval(start + "/" + end);
    coordinator.unloadSegmentsForDataSource(dataSource, interval);
    RetryUtil.retryUntilFalse(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return coordinator.areSegmentsLoaded(dataSource);
          }
        }, "Segment Unloading"
    );
    coordinator.deleteSegmentsDataSource(dataSource, interval);
    RetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return (indexer.getPendingTasks().size() + indexer.getRunningTasks().size() + indexer.getWaitingTasks()
                                                                                                 .size()) == 0;
          }
        }, "Waiting for Tasks Completion"
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
