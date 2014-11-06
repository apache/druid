/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.tests.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.testing.clients.CoordinatorResourceTestClient;
import io.druid.testing.clients.OverlordResourceTestClient;
import io.druid.testing.utils.FromFileTestQueryHelper;
import io.druid.testing.utils.RetryUtil;
import org.apache.commons.io.IOUtils;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.Callable;

public abstract class AbstractIndexerTest
{

  @Inject
  protected CoordinatorResourceTestClient coordinator;
  @Inject
  protected OverlordResourceTestClient indexer;
  @Inject
  protected ObjectMapper jsonMapper;

  @Inject
  protected FromFileTestQueryHelper queryHelper;

  protected void unloadAndKillData(final String dataSource) throws Exception
  {
    Interval interval = new Interval("2013-01-01T00:00:00.000Z/2013-12-01T00:00:00.000Z");
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
    InputStream inputStream = ITRealtimeIndexTaskTest.class.getResourceAsStream(file);
    StringWriter writer = new StringWriter();
    IOUtils.copy(inputStream, writer, "UTF-8");
    return writer.toString();
  }

}
