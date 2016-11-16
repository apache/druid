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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidServer;
import io.druid.client.DruidServerConfig;
import io.druid.client.FilteredServerInventoryView;
import io.druid.client.ServerView;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.MapQueryToolChestWarehouse;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.RegexDataSource;
import io.druid.query.SegmentDescriptor;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.security.AuthConfig;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 *
 */
public class BrokerQueryResourceTest
{
  private static final QueryToolChestWarehouse warehouse = new MapQueryToolChestWarehouse(ImmutableMap.<Class<? extends Query>, QueryToolChest>of());
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  public static final ServerConfig serverConfig = new ServerConfig()
  {
    @Override
    public int getNumThreads()
    {
      return 1;
    }

    @Override
    public Period getMaxIdleTime()
    {
      return Period.seconds(1);
    }
  };
  public static final QuerySegmentWalker testSegmentWalker = new QuerySegmentWalker()
  {
    @Override
    public <T> QueryRunner<T> getQueryRunnerForIntervals(
        Query<T> query, Iterable<Interval> intervals
    )
    {
      return new QueryRunner<T>()
      {
        @Override
        public Sequence<T> run(
            Query<T> query, Map<String, Object> responseContext
        )
        {
          return Sequences.<T>empty();
        }
      };
    }

    @Override
    public <T> QueryRunner<T> getQueryRunnerForSegments(
        Query<T> query, Iterable<SegmentDescriptor> specs
    )
    {
      return getQueryRunnerForIntervals(null, null);
    }
  };

  private static final FilteredServerInventoryView serverView = new FilteredServerInventoryView()
  {
    Map<String, DruidServer> view = Maps.newHashMap();
    {
      DruidServerConfig conf = new DruidServerConfig();
      Interval iv1 = new Interval(0, 1000);
      Interval iv2 = new Interval(1000, 2000);
      Interval iv3 = new Interval(2000, 3000);
      DruidServer server1 = new DruidServer(new DruidNode("test", "localhost", 1000), conf, "historical");
      server1.addDataSegment("ds1_1", new DataSegment("ds1", iv1, "v1", null, null, null, null, 0, 100));
      server1.addDataSegment("ds1_2", new DataSegment("ds1", iv2, "v1", null, null, null, null, 0, 200));
      server1.addDataSegment("ds2_1", new DataSegment("ds2", iv1, "v1", null, null, null, null, 0, 300));
      server1.addDataSegment("ds2_2", new DataSegment("ds2", iv2, "v1", null, null, null, null, 0, 400));

      DruidServer server2 = new DruidServer(new DruidNode("test", "localhost", 2000), conf, "realtime");
      server2.addDataSegment("ds2_3", new DataSegment("ds2", iv3, "v1", null, null, null, null, 0, 110));
      server2.addDataSegment("ds3_1", new DataSegment("ds3", iv1, "v1", null, null, null, null, 0, 210));
      server2.addDataSegment("ds3_2", new DataSegment("ds3", iv2, "v1", null, null, null, null, 0, 310));

      view.put("server1", server1);
      view.put("server2", server2);
    }

    @Override
    public void registerSegmentCallback(
        Executor exec, ServerView.SegmentCallback callback, Predicate<Pair<DruidServerMetadata, DataSegment>> filter
    )
    {
    }

    @Override
    public void registerServerCallback(Executor exec, ServerView.ServerCallback callback)
    {
    }

    @Override
    public DruidServer getInventoryValue(String string)
    {
      return view.get(string);
    }

    @Override
    public Iterable<DruidServer> getInventory()
    {
      return view.values();
    }
  };

  private static final ServiceEmitter noopServiceEmitter = new NoopServiceEmitter();

  private BrokerQueryResource queryResource;
  private QueryManager queryManager;

  @BeforeClass
  public static void staticSetup()
  {
    com.metamx.emitter.EmittingLogger.registerEmitter(noopServiceEmitter);
  }

  @Before
  public void setup()
  {
    queryManager = new QueryManager();
    queryResource = new BrokerQueryResource(
        warehouse,
        serverConfig,
        jsonMapper,
        jsonMapper,
        testSegmentWalker,
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        queryManager,
        new AuthConfig(),
        serverView,
        null
    );
  }

  @Test
  public void testRegexDataSource() throws Exception
  {
    SelectQuery q = new Druids.SelectQueryBuilder()
        .dataSource(new RegexDataSource(Arrays.asList("ds[13]")))
        .intervals(Arrays.asList(new Interval(0, 3000)))
        .pagingSpec(PagingSpec.newSpec(1)).build();
    Query rewritten = queryResource.prepareQuery(q);
    List<String> names = Lists.newArrayList(rewritten.getDataSource().getNames());
    Collections.sort(names);
    Assert.assertEquals(ImmutableList.of("ds1", "ds3"), names);

    q = q.withDataSource(new RegexDataSource(Arrays.asList("ds.*")));
    rewritten = queryResource.prepareQuery(q);
    names = Lists.newArrayList(rewritten.getDataSource().getNames());
    Collections.sort(names);
    Assert.assertEquals(ImmutableList.of("ds1", "ds2", "ds3"), names);
  }
}
