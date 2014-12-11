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

package io.druid.query.datasourcemetadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedInputRow;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.TestIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class DataSourceMetadataQueryTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testQuerySerialization() throws IOException
  {
    Query query = Druids.newDataSourceMetadataQueryBuilder()
                        .dataSource("testing")
                        .build();

    String json = jsonMapper.writeValueAsString(query);
    Query serdeQuery = jsonMapper.readValue(json, Query.class);

    Assert.assertEquals(query, serdeQuery);
  }

  @Test
  public void testContextSerde() throws Exception
  {
    final DataSourceMetadataQuery query = Druids.newDataSourceMetadataQueryBuilder()
                                            .dataSource("foo")
                                            .intervals("2013/2014")
                                            .context(
                                                ImmutableMap.<String, Object>of(
                                                    "priority",
                                                    1,
                                                    "useCache",
                                                    true,
                                                    "populateCache",
                                                    true,
                                                    "finalize",
                                                    true
                                                )
                                            ).build();

    final ObjectMapper mapper = new DefaultObjectMapper();

    final Query serdeQuery = mapper.readValue(
        mapper.writeValueAsBytes(
            mapper.readValue(
                mapper.writeValueAsString(
                    query
                ), Query.class
            )
        ), Query.class
    );

    Assert.assertEquals(1, serdeQuery.getContextValue("priority"));
    Assert.assertEquals(true, serdeQuery.getContextValue("useCache"));
    Assert.assertEquals(true, serdeQuery.getContextValue("populateCache"));
    Assert.assertEquals(true, serdeQuery.getContextValue("finalize"));
  }

  @Test
  public void testMaxIngestedEventTime() throws Exception
  {
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex(false);
    final QueryRunner runner = QueryRunnerTestHelper.makeQueryRunner(
        (QueryRunnerFactory) new DataSourceMetadataQueryRunnerFactory(
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ), new IncrementalIndexSegment(rtIndex, "test")
    );
    DateTime timestamp = new DateTime(System.currentTimeMillis());
    rtIndex.add(
        new MapBasedInputRow(
            timestamp.getMillis(),
            ImmutableList.of("dim1"),
            ImmutableMap.<String, Object>of("dim1", "x")
        )
    );
    DataSourceMetadataQuery dataSourceMetadataQuery = Druids.newDataSourceMetadataQueryBuilder()
                                                    .dataSource("testing")
                                                    .build();
    Map<String, Object> context = new MapMaker().makeMap();
    context.put(Result.MISSING_SEGMENTS_KEY, Lists.newArrayList());
    Iterable<Result<DataSourceMetadataResultValue>> results = Sequences.toList(
        runner.run(dataSourceMetadataQuery, context),
        Lists.<Result<DataSourceMetadataResultValue>>newArrayList()
    );
    DataSourceMetadataResultValue val = results.iterator().next().getValue();
    DateTime maxIngestedEventTime = val.getMaxIngestedEventTime();

    Assert.assertEquals(timestamp, maxIngestedEventTime);
  }

  @Test
  public void testFilterSegments()
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        null,
        Arrays.asList(
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01/P1D");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T01/PT1H");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T02/PT1H");
              }
            }
        )
    );

    Assert.assertEquals(segments.size(), 3);

    List<LogicalSegment> expected = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T02/PT1H");
          }
        }
    );

    for (int i = 0; i < segments.size(); i++) {
      Assert.assertEquals(segments.get(i).getInterval(), expected.get(i).getInterval());
    }
  }

}
