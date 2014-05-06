/*
 * Druid - a distributed column store.
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
 *
 * This file Copyright (C) 2014 N3TWORK, Inc. and contributed to the Druid project
 * under the Druid Corporate Contributor License Agreement.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static io.druid.query.QueryRunnerTestHelper.addRowsIndexConstant;
import static io.druid.query.QueryRunnerTestHelper.allGran;
import static io.druid.query.QueryRunnerTestHelper.commonAggregators;
import static io.druid.query.QueryRunnerTestHelper.dataSource;
import static io.druid.query.QueryRunnerTestHelper.fullOnInterval;
import static io.druid.query.QueryRunnerTestHelper.indexMetric;
import static io.druid.query.QueryRunnerTestHelper.providerDimension;

public class TopNQueryTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(4)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

}
