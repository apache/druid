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

package org.apache.druid.tests.query;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.clients.CoordinatorResourceTestClient;
import org.apache.druid.testing.clients.QueryResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.testing.utils.TestQueryHelper;
import org.apache.druid.tests.TestNGGroup;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Future;

@Test(groups = {TestNGGroup.QUERY, TestNGGroup.CENTRALIZED_DATASOURCE_SCHEMA})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITWikipediaQueryTest
{
  private static final Logger LOG = new Logger(ITWikipediaQueryTest.class);

  public static final String WIKIPEDIA_DATA_SOURCE = "wikipedia_editstream";
  private static final String WIKI_LOOKUP = "wiki-simple";
  private static final String WIKIPEDIA_QUERIES_RESOURCE = "/queries/wikipedia_editstream_queries.json";
  private static final String WIKIPEDIA_LOOKUP_RESOURCE = "/queries/wiki-lookup-config.json";

  @Inject
  private CoordinatorResourceTestClient coordinatorClient;
  @Inject
  private TestQueryHelper queryHelper;
  @Inject
  private QueryResourceTestClient queryClient;
  @Inject
  private IntegrationTestingConfig config;

  @BeforeMethod
  public void before() throws Exception
  {
    // ensure that wikipedia segments are loaded completely
    ITRetryUtil.retryUntilTrue(
        () -> coordinatorClient.areSegmentsLoaded(WIKIPEDIA_DATA_SOURCE), "wikipedia segment load"
    );
    if (!coordinatorClient.areLookupsLoaded(WIKI_LOOKUP)) {
      coordinatorClient.initializeLookups(WIKIPEDIA_LOOKUP_RESOURCE);
      ITRetryUtil.retryUntilTrue(
          () -> coordinatorClient.areLookupsLoaded(WIKI_LOOKUP), "wikipedia lookup load"
      );
    }
  }

  /**
   * A combination of request Content-Type and Accept HTTP header
   * The first is Content-Type which can not be null while the 2nd is Accept which could be null
   * <p>
   * When Accept is null, its value defaults to value of Content-Type
   */
  @DataProvider
  public static Object[][] encodingCombination()
  {
    return new Object[][]{
        {MediaType.APPLICATION_JSON, null},
        {MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON},
        {MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE},
        {SmileMediaTypes.APPLICATION_JACKSON_SMILE, null},
        {SmileMediaTypes.APPLICATION_JACKSON_SMILE, MediaType.APPLICATION_JSON},
        {SmileMediaTypes.APPLICATION_JACKSON_SMILE, SmileMediaTypes.APPLICATION_JACKSON_SMILE},
        };
  }

  @Test(dataProvider = "encodingCombination")
  public void testWikipediaQueriesFromFile(String contentType, String accept)
      throws Exception
  {
    // run tests on a new query helper
    TestQueryHelper queryHelper = this.queryHelper.withEncoding(contentType, accept);

    queryHelper.testQueriesFromFile(WIKIPEDIA_QUERIES_RESOURCE);
  }

  @Test
  public void testQueryLaningLaneIsLimited() throws Exception
  {
    ITRetryUtil.retryUntil(
        () -> {
          // the broker is configured with a manually defined query lane, 'one' with limit 1
          //  -Ddruid.query.scheduler.laning.type=manual
          //  -Ddruid.query.scheduler.laning.lanes.one=1
          // by issuing 50 queries, at least 1 of them will succeed on 'one', and at least 1 of them will overlap enough to
          // get limited.
          // It's possible but unlikely that these queries execute in a way that none of them overlap, so we
          // retry this test a few times to compensate for this.
          final int numQueries = 50;
          List<Future<StatusResponseHolder>> futures = new ArrayList<>(numQueries);
          for (int i = 0; i < numQueries; i++) {
            futures.add(
                queryClient.queryAsync(
                    queryHelper.getQueryURL(config.getBrokerUrl()),
                    getQueryBuilder().build()
                )
            );
          }

          int success = 0;
          int limited = 0;

          for (Future<StatusResponseHolder> future : futures) {
            StatusResponseHolder status = future.get();
            if (status.getStatus().getCode() == QueryCapacityExceededException.STATUS_CODE) {
              limited++;
              Assert.assertTrue(status.getContent().contains(QueryCapacityExceededException.makeLaneErrorMessage("one", 1)));
            } else if (status.getStatus().getCode() == HttpResponseStatus.OK.getCode()) {
              success++;
            }
          }

          try {
            Assert.assertTrue(success > 0);
            Assert.assertTrue(limited > 0);
            return true;
          }
          catch (AssertionError ae) {
            LOG.error(ae, "Got assertion error in testQueryLaningLaneIsLimited");
            return false;
          }
        },
        true,
        5000,
        3,
        "testQueryLaningLaneIsLimited"
    );

    // test another to make sure we can still issue one query at a time
    StatusResponseHolder followUp = queryClient.queryAsync(
        queryHelper.getQueryURL(config.getBrokerUrl()),
        getQueryBuilder().build()
    ).get();

    Assert.assertEquals(followUp.getStatus().getCode(), HttpResponseStatus.OK.getCode());

    StatusResponseHolder andAnother = queryClient.queryAsync(
        queryHelper.getQueryURL(config.getBrokerUrl()),
        getQueryBuilder().build()
    ).get();

    Assert.assertEquals(andAnother.getStatus().getCode(), HttpResponseStatus.OK.getCode());
  }

  @Test
  public void testQueryLaningWithNoLane() throws Exception
  {
    // the broker is configured with a manually defined query lane, 'one' with limit 1
    //  -Ddruid.query.scheduler.laning.type=manual
    //  -Ddruid.query.scheduler.laning.lanes.one=1
    // these queries will not belong to the lane so none of them should be limited
    final int numQueries = 50;
    List<Future<StatusResponseHolder>> futures = new ArrayList<>(numQueries);
    for (int i = 0; i < numQueries; i++) {
      futures.add(
          queryClient.queryAsync(
              queryHelper.getQueryURL(config.getBrokerUrl()),
              getQueryBuilder().context(ImmutableMap.of("queryId", UUID.randomUUID().toString())).build()
          )
      );
    }

    int success = 0;
    int limited = 0;

    for (Future<StatusResponseHolder> future : futures) {
      StatusResponseHolder status = future.get();
      if (status.getStatus().getCode() == QueryCapacityExceededException.STATUS_CODE) {
        limited++;
      } else if (status.getStatus().getCode() == HttpResponseStatus.OK.getCode()) {
        success++;
      }
    }

    Assert.assertTrue(success > 0);
    Assert.assertEquals(limited, 0);

  }

  private Druids.TimeseriesQueryBuilder getQueryBuilder()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource("wikipedia_editstream")
                 .aggregators(new CountAggregatorFactory("chocula"))
                 .intervals("2013-01-01T00:00:00.000/2013-01-08T00:00:00.000")
                 .context(ImmutableMap.of("lane", "one", "queryId", UUID.randomUUID().toString()));
  }
}
