/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.NoopRequestLogger;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class QueryResourceTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  public static final ServerConfig serverConfig = new ServerConfig(){
    @Override
    public int getNumThreads(){
      return 1;
    }
    @Override
    public Period getMaxIdleTime(){
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
  public static void staticSetup(){
    com.metamx.emitter.EmittingLogger.registerEmitter(noopServiceEmitter);
  }
  @Before
  public void setup()
  {
    EasyMock.expect(testServletRequest.getContentType()).andReturn(MediaType.APPLICATION_JSON);
    EasyMock.expect(testServletRequest.getRemoteAddr()).andReturn("localhost").anyTimes();
    EasyMock.replay(testServletRequest);
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
    Response respone = queryResource.doPost(new ByteArrayInputStream(simpleTimeSeriesQuery.getBytes("UTF-8")), null /*pretty*/, testServletRequest);
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
    Response respone = queryResource.doPost(new ByteArrayInputStream("Meka Leka Hi Meka Hiney Ho".getBytes("UTF-8")), null /*pretty*/, testServletRequest);
    Assert.assertNotNull(respone);
    Assert.assertEquals(Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(), respone.getStatus());
  }
}
