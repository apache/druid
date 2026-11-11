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

package org.apache.druid.server.router;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.sql.http.SqlQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;





public class ManualTieredBrokerSelectorStrategyTest
{
  private TieredBrokerConfig tieredBrokerConfig;
  private Druids.TimeseriesQueryBuilder queryBuilder;

  @BeforeEach
  public void setup()
  {
    tieredBrokerConfig = new TieredBrokerConfig()
    {
      @Override
      public String getDefaultBrokerServiceName()
      {
        return Names.BROKER_SVC_HOT;
      }

      @Override
      public LinkedHashMap<String, String> getTierToBrokerMap()
      {
        LinkedHashMap<String, String> tierToBrokerMap = new LinkedHashMap<>();
        tierToBrokerMap.put("hotTier", Names.BROKER_SVC_HOT);
        tierToBrokerMap.put("mediumTier", Names.BROKER_SVC_MEDIUM);
        tierToBrokerMap.put("coldTier", Names.BROKER_SVC_COLD);

        return tierToBrokerMap;
      }
    };

    queryBuilder =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .intervals(
                  new MultipleIntervalSegmentSpec(
                      Collections.singletonList(Intervals.of("2009/2010"))
                  )
              );
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper mapper = new DefaultObjectMapper();

    String json = "{\"type\":\"manual\"}";
    TieredBrokerSelectorStrategy strategy = mapper.readValue(
        json,
        TieredBrokerSelectorStrategy.class
    );
    Assertions.assertTrue(strategy instanceof ManualTieredBrokerSelectorStrategy);

    ManualTieredBrokerSelectorStrategy queryContextStrategy =
        (ManualTieredBrokerSelectorStrategy) strategy;
    Assertions.assertNull(queryContextStrategy.getDefaultManualBrokerService());

    json = "{\"type\":\"manual\",\"defaultManualBrokerService\":\"hotBroker\"}";
    queryContextStrategy = mapper.readValue(
        json,
        ManualTieredBrokerSelectorStrategy.class
    );
    Assertions.assertEquals(queryContextStrategy.getDefaultManualBrokerService(), "hotBroker");
  }

  @Test
  public void testGetBrokerServiceName()
  {
    final ManualTieredBrokerSelectorStrategy strategy =
        new ManualTieredBrokerSelectorStrategy(null);

    Assertions.assertEquals(
        Optional.absent(),
        strategy.getBrokerServiceName(tieredBrokerConfig, queryBuilder.build())
    );
    Assertions.assertEquals(
        Optional.absent(),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.INVALID_BROKER))
                .build()
        )
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_HOT),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.BROKER_SVC_HOT))
                .build()
        )
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_COLD),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.BROKER_SVC_COLD))
                .build()
        )
    );
  }

  @Test
  public void testGetBrokerServiceName_withFallback()
  {
    final ManualTieredBrokerSelectorStrategy strategy =
        new ManualTieredBrokerSelectorStrategy(Names.BROKER_SVC_MEDIUM);

    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_MEDIUM),
        strategy.getBrokerServiceName(tieredBrokerConfig, queryBuilder.build())
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_MEDIUM),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.INVALID_BROKER))
                .build()
        )
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_HOT),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.BROKER_SVC_HOT))
                .build()
        )
    );
  }

  @Test
  public void testGetBrokerServiceName_withInvalidFallback()
  {
    final ManualTieredBrokerSelectorStrategy strategy =
        new ManualTieredBrokerSelectorStrategy("noSuchBroker");

    Assertions.assertEquals(
        Optional.absent(),
        strategy.getBrokerServiceName(tieredBrokerConfig, queryBuilder.build())
    );
    Assertions.assertEquals(
        Optional.absent(),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.INVALID_BROKER))
                .build()
        )
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_HOT),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            queryBuilder
                .context(QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.BROKER_SVC_HOT))
                .build()
        )
    );
  }

  @Test
  public void testGetBrokerServiceName_forSql()
  {
    final ManualTieredBrokerSelectorStrategy strategy =
        new ManualTieredBrokerSelectorStrategy(null);

    Assertions.assertEquals(
        Optional.absent(),
        strategy.getBrokerServiceName(tieredBrokerConfig, createSqlQueryWithContext(null))
    );
    Assertions.assertEquals(
        Optional.absent(),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            createSqlQueryWithContext(
                QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.INVALID_BROKER)
            )
        )
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_HOT),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            createSqlQueryWithContext(
                QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.BROKER_SVC_HOT)
            )
        )
    );
    Assertions.assertEquals(
        Optional.of(Names.BROKER_SVC_COLD),
        strategy.getBrokerServiceName(
            tieredBrokerConfig,
            createSqlQueryWithContext(
                QueryContext.ofMap(QueryContextParameters.BROKER_SERVICE, Names.BROKER_SVC_COLD)
            )
        )
    );
  }

  private SqlQuery createSqlQueryWithContext(Map<String, Object> queryContext)
  {
    return new SqlQuery(
        "SELECT * FROM test",
        null,
        false,
        false,
        false,
        queryContext,
        null
    );
  }

  /**
   * Test constants.
   */
  private static class Names
  {
    static final String BROKER_SVC_HOT = "druid/hotBroker";
    static final String BROKER_SVC_MEDIUM = "druid/mediumBroker";
    static final String BROKER_SVC_COLD = "druid/coldBroker";

    static final String INVALID_BROKER = "invalidBroker";
  }
}
