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

package io.druid.tests.query;

import com.google.inject.Inject;
import io.druid.testing.clients.CoordinatorResourceTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.FromFileTestQueryHelper;
import io.druid.testing.utils.RetryUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITWikipediaQueryTest
{
  private static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";
  private static final String WIKIPEDIA_QUERIES_RESOURCE = "/queries/wikipedia_editstream_queries.json";
  @Inject
  private CoordinatorResourceTestClient coordinatorClient;
  @Inject
  private FromFileTestQueryHelper queryHelper;

  @BeforeMethod
  public void before()
  {
    // ensure that twitter segments are loaded completely
    RetryUtil.retryUntilTrue(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return coordinatorClient.areSegmentsLoaded(WIKIPEDIA_DATA_SOURCE);
          }
        }, "wikipedia segment load"
    );
  }

  @Test
  public void testQueriesFromFile() throws Exception
  {
    queryHelper.testQueriesFromFile(WIKIPEDIA_QUERIES_RESOURCE, 2);
  }

}
