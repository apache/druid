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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.nio.charset.StandardCharsets;

public class HttpIndexingServiceClientTest
{
  private HttpIndexingServiceClient httpIndexingServiceClient;
  private ObjectMapper jsonMapper;
  private DruidLeaderClient druidLeaderClient;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);
    httpIndexingServiceClient = new HttpIndexingServiceClient(
        jsonMapper,
        druidLeaderClient
    );
  }

  @Test
  public void testSample() throws Exception
  {
    final SamplerResponse samplerResponse = new SamplerResponse(
        2,
        2,
        ImmutableList.of(
            new SamplerResponse.SamplerResponseRow(
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                false,
                null
            )
        )
    );

    final SamplerSpec samplerSpec = new SamplerSpec()
    {
      @Override
      public SamplerResponse sample()
      {
        return samplerResponse;
      }
    };

    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(response.getContent()).andReturn(new BigEndianHeapChannelBuffer(0));
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        HttpResponseStatus.OK,
        response,
        StandardCharsets.UTF_8
    ).addChunk(jsonMapper.writeValueAsString(samplerResponse));

    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/sampler"))
            .andReturn(new Request(HttpMethod.POST, new URL("http://localhost:8090/druid/indexer/v1/sampler")))
            .anyTimes();
    EasyMock.replay(druidLeaderClient);

    final SamplerResponse actualResponse = httpIndexingServiceClient.sample(samplerSpec);
    Assert.assertEquals(samplerResponse, actualResponse);

    EasyMock.verify(druidLeaderClient, response);
  }

  @Test
  public void testSampleError() throws Exception
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("Failed to sample with sampler spec");
    expectedException.expectMessage("Please check overlord log");

    final SamplerResponse samplerResponse = new SamplerResponse(
        2,
        2,
        ImmutableList.of(
            new SamplerResponse.SamplerResponseRow(
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                ImmutableMap.of("time", "2020-01-01", "x", "123", "y", "456"),
                false,
                null
            )
        )
    );

    final SamplerSpec samplerSpec = new SamplerSpec()
    {
      @Override
      public SamplerResponse sample()
      {
        return samplerResponse;
      }
    };
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(response.getContent()).andReturn(new BigEndianHeapChannelBuffer(0));
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        HttpResponseStatus.INTERNAL_SERVER_ERROR,
        response,
        StandardCharsets.UTF_8
    ).addChunk("");

    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/sampler"))
            .andReturn(new Request(HttpMethod.POST, new URL("http://localhost:8090/druid/indexer/v1/sampler")))
            .anyTimes();
    EasyMock.replay(druidLeaderClient);


    httpIndexingServiceClient.sample(samplerSpec);
    EasyMock.verify(druidLeaderClient, response);
  }
}
