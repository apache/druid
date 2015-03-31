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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.StringUtils;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.jackson.JacksonModule;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import io.druid.server.namespace.cache.NamespaceExtractionCacheManager;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class QueryResourceTest
{
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
  private final HttpServletRequest testServletRequest = EasyMock.createMock(HttpServletRequest.class);
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

  private static final ServiceEmitter noopServiceEmitter = new NoopServiceEmitter();

  @BeforeClass
  public static void staticSetup()
  {
    com.metamx.emitter.EmittingLogger.registerEmitter(noopServiceEmitter);
  }

  private NamespaceExtractionCacheManager namespaceExtractionCacheManager;
  private Lifecycle lifecycle;

  @Before
  public void setup()
  {
    EasyMock.expect(testServletRequest.getContentType()).andReturn(MediaType.APPLICATION_JSON);
    EasyMock.expect(testServletRequest.getRemoteAddr()).andReturn("localhost").anyTimes();
    EasyMock.replay(testServletRequest);
    lifecycle = new Lifecycle();
    namespaceExtractionCacheManager = new NamespaceExtractionCacheManager(lifecycle)
    {
      @Override
      public ConcurrentMap<String, String> getCacheMap(String namespace)
      {
        return new ConcurrentHashMap<>();
      }

      @Override
      public Collection<String> getKnownNamespaces()
      {
        return ImmutableList.of();
      }
    };
  }

  @After
  public void tearDown()
  {
    lifecycle.stop();
  }

  private static final String simpleTimeSeriesQuery = "{\n"
                                                      + "    \"queryType\": \"timeseries\",\n"
                                                      + "    \"dataSource\": \"mmx_metrics\",\n"
                                                      + "    \"granularity\": \"hour\",\n"
                                                      + "    \"intervals\": [\n"
                                                      + "      \"2014-12-17/2015-12-30\"\n"
                                                      + "    ],\n"
                                                      + "    \"aggregations\": [\n"
                                                      + "      {\n"
                                                      + "        \"type\": \"count\",\n"
                                                      + "        \"name\": \"rows\"\n"
                                                      + "      }\n"
                                                      + "    ]\n"
                                                      + "}";

  @Test
  public void testGoodQuery() throws IOException
  {
    QueryResource queryResource = new QueryResource(
        serverConfig,
        jsonMapper,
        jsonMapper,
        testSegmentWalker,
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        new QueryManager()
    );
    Response respone = queryResource.doPost(
        new ByteArrayInputStream(simpleTimeSeriesQuery.getBytes("UTF-8")),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(respone);
  }

  @Test
  public void testBadQuery() throws IOException
  {

    QueryResource queryResource = new QueryResource(
        serverConfig,
        jsonMapper,
        jsonMapper,
        testSegmentWalker,
        new NoopServiceEmitter(),
        new NoopRequestLogger(),
        new QueryManager()
    );
    Response respone = queryResource.doPost(
        new ByteArrayInputStream("Meka Leka Hi Meka Hiney Ho".getBytes("UTF-8")),
        null /*pretty*/,
        testServletRequest
    );
    Assert.assertNotNull(respone);
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), respone.getStatus());
  }
}
