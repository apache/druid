/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.tests.query;

import com.google.inject.Inject;
import io.druid.testing.clients.CoordinatorResourceTestClient;
import io.druid.testing.guice.DruidTestModuleFactory;
import io.druid.testing.utils.RetryUtil;
import io.druid.testing.utils.TestQueryHelper;
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
  private TestQueryHelper queryHelper;

  @BeforeMethod
  public void before()
  {
    // ensure that wikipedia segments are loaded completely
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
  public void testWikipediaQueriesFromFile() throws Exception
  {
    queryHelper.testQueriesFromFile(WIKIPEDIA_QUERIES_RESOURCE, 2);
  }

}
