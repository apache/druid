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

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITIndexerTest extends AbstractIndexerTest
{
  private static final Logger LOG = new Logger(ITIndexerTest.class);
  private static String INDEX_TASK = "/indexer/wikipedia_index_task.json";
  private static String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static String INDEX_DATASOURCE = "wikipedia_index_test";


  @Inject
  private IntegrationTestingConfig config;

  @Test
  public void testIndexData() throws Exception
  {
    loadData();
    try {
      queryHelper.testQueriesFromFile(INDEX_QUERIES_RESOURCE, 2);
    }
    catch (Exception e) {
      e.printStackTrace();
      Throwables.propagate(e);
    }
    finally {
      unloadAndKillData(INDEX_DATASOURCE);
    }

  }

  private void loadData() throws Exception
  {
    final String taskID = indexer.submitTask(getTaskAsString(INDEX_TASK));
    LOG.info("TaskID for loading index task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    RetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return coordinator.areSegmentsLoaded(INDEX_DATASOURCE);
          }
        }, "Segment Load"
    );
  }


}
