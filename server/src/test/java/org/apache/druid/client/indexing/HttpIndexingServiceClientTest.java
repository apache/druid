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
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URL;
import java.util.Map;

public class HttpIndexingServiceClientTest
{
  private HttpIndexingServiceClient httpIndexingServiceClient;
  private ObjectMapper jsonMapper;
  private DruidLeaderClient druidLeaderClient;
  private ObjectMapper mockMapper;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup()
  {
    jsonMapper = new DefaultObjectMapper();
    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);
    mockMapper = EasyMock.createMock(ObjectMapper.class);

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
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        jsonMapper.writeValueAsString(samplerResponse)
    );

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
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR).anyTimes();
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(response, "");

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

  @Test
  public void testGetTaskReport() throws Exception
  {
    String taskId = "testTaskId";
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.replay(response);

    Map<String, Object> dummyResponse = ImmutableMap.of("test", "value");

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        jsonMapper.writeValueAsString(dummyResponse)
    );

    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/indexer/v1/task/testTaskId/reports"))
            .andReturn(new Request(
                HttpMethod.GET,
                new URL("http://localhost:8090/druid/indexer/v1/task/testTaskId/reports")
            ))
            .anyTimes();
    EasyMock.replay(druidLeaderClient);

    final Map<String, Object> actualResponse = httpIndexingServiceClient.getTaskReport(taskId);
    Assert.assertEquals(dummyResponse, actualResponse);

    EasyMock.verify(druidLeaderClient, response);
  }

  @Test
  public void testGetTaskReportStatusNotFound() throws Exception
  {
    String taskId = "testTaskId";
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    String errorMsg = "No task reports were found for this task. "
                      + "The task may not exist, or it may not have completed yet.";
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.NOT_FOUND).anyTimes();
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        errorMsg
    );

    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/indexer/v1/task/testTaskId/reports"))
            .andReturn(new Request(
                HttpMethod.GET,
                new URL("http://localhost:8090/druid/indexer/v1/task/testTaskId/reports")
            ))
            .anyTimes();
    EasyMock.replay(druidLeaderClient);

    final Map<String, Object> actualResponse = httpIndexingServiceClient.getTaskReport(taskId);
    Assert.assertNull(actualResponse);

    EasyMock.verify(druidLeaderClient, response);
  }

  @Test
  public void testGetTaskReportEmpty() throws Exception
  {
    String taskId = "testTaskId";
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        ""
    );

    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/indexer/v1/task/testTaskId/reports"))
            .andReturn(new Request(
                HttpMethod.GET,
                new URL("http://localhost:8090/druid/indexer/v1/task/testTaskId/reports")
            ))
            .anyTimes();
    EasyMock.replay(druidLeaderClient);

    final Map<String, Object> actualResponse = httpIndexingServiceClient.getTaskReport(taskId);
    Assert.assertNull(actualResponse);

    EasyMock.verify(druidLeaderClient, response);
  }

  @Test
  public void testCompact() throws Exception
  {
    DataSegment segment = new DataSegment(
        "test",
        Intervals.of("2015-04-12/2015-04-13"),
        "1",
        ImmutableMap.of("bucket", "bucket", "path", "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip"),
        null,
        null,
        NoneShardSpec.instance(),
        0,
        1
    );
    Capture captureTask = EasyMock.newCapture();
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        jsonMapper.writeValueAsString(ImmutableMap.of("task", "aaa"))
    );

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/task"))
            .andReturn(new Request(HttpMethod.POST, new URL("http://localhost:8090/druid/indexer/v1/task")))
            .anyTimes();
    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();
    EasyMock.expect(mockMapper.writeValueAsBytes(EasyMock.capture(captureTask)))
            .andReturn(new byte[]{1, 2, 3})
            .anyTimes();
    EasyMock.expect(mockMapper.readValue(EasyMock.anyString(), EasyMock.eq(JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)))
            .andReturn(ImmutableMap.of())
            .anyTimes();
    EasyMock.replay(druidLeaderClient, mockMapper);

    HttpIndexingServiceClient httpIndexingServiceClient = new HttpIndexingServiceClient(
        mockMapper,
        druidLeaderClient
    );

    try {
      httpIndexingServiceClient.compactSegments(
          "test-compact",
          ImmutableList.of(segment),
          50,
          null,
          null,
          null,
          null,
          null,
          null,
          null
      );
    }
    catch (Exception e) {
      // Ignore IllegalStateException as taskId is internally generated and returned task id will failed check
      Assert.assertEquals(IllegalStateException.class.getName(), e.getCause().getClass().getName());
    }
    ClientCompactionTaskQuery taskQuery = (ClientCompactionTaskQuery) captureTask.getValue();
    Assert.assertEquals(Intervals.of("2015-04-12/2015-04-13"), taskQuery.getIoConfig().getInputSpec().getInterval());
    Assert.assertNull(taskQuery.getGranularitySpec());
    Assert.assertNull(taskQuery.getIoConfig().getInputSpec().getSha256OfSortedSegmentIds());
  }

  @Test
  public void testCompactWithSegmentGranularity() throws Exception
  {
    DataSegment segment = new DataSegment(
        "test",
        Intervals.of("2015-04-12/2015-04-13"),
        "1",
        ImmutableMap.of("bucket", "bucket", "path", "test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip"),
        null,
        null,
        NoneShardSpec.instance(),
        0,
        1
    );
    Capture captureTask = EasyMock.newCapture();
    HttpResponse response = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(response.status()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        jsonMapper.writeValueAsString(ImmutableMap.of("task", "aaa"))
    );

    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/task"))
            .andReturn(new Request(HttpMethod.POST, new URL("http://localhost:8090/druid/indexer/v1/task")))
            .anyTimes();
    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(responseHolder)
            .anyTimes();
    EasyMock.expect(mockMapper.writeValueAsBytes(EasyMock.capture(captureTask)))
            .andReturn(new byte[]{1, 2, 3})
            .anyTimes();
    EasyMock.expect(mockMapper.readValue(EasyMock.anyString(), EasyMock.eq(JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT)))
            .andReturn(ImmutableMap.of())
            .anyTimes();
    EasyMock.replay(druidLeaderClient, mockMapper);

    HttpIndexingServiceClient httpIndexingServiceClient = new HttpIndexingServiceClient(
        mockMapper,
        druidLeaderClient
    );

    try {
      httpIndexingServiceClient.compactSegments(
          "test-compact",
          ImmutableList.of(segment),
          50,
          null,
          new ClientCompactionTaskGranularitySpec(Granularities.YEAR, null, null),
          null,
          null,
          null,
          null,
          null
      );
    }
    catch (Exception e) {
      // Ignore IllegalStateException as taskId is internally generated and returned task id will failed check
      Assert.assertEquals(IllegalStateException.class.getName(), e.getCause().getClass().getName());
    }
    ClientCompactionTaskQuery taskQuery = (ClientCompactionTaskQuery) captureTask.getValue();
    Assert.assertEquals(Intervals.of("2015-01-01/2016-01-01"), taskQuery.getIoConfig().getInputSpec().getInterval());
    Assert.assertEquals(Granularities.YEAR, taskQuery.getGranularitySpec().getSegmentGranularity());
    Assert.assertNull(taskQuery.getIoConfig().getInputSpec().getSha256OfSortedSegmentIds());
  }

  @Test
  public void testGetTotalWorkerCapacityWithAutoScale() throws Exception
  {
    int currentClusterCapacity = 5;
    int maximumCapacityWithAutoScale = 10;
    // Mock response for /druid/indexer/v1/totalWorkerCapacity
    HttpResponse totalWorkerCapacityResponse = EasyMock.createMock(HttpResponse.class);
    EasyMock.expect(totalWorkerCapacityResponse.status()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.replay(totalWorkerCapacityResponse);
    IndexingTotalWorkerCapacityInfo indexingTotalWorkerCapacityInfo = new IndexingTotalWorkerCapacityInfo(
        currentClusterCapacity,
        maximumCapacityWithAutoScale
    );
    StringFullResponseHolder autoScaleResponseHolder = new StringFullResponseHolder(
        totalWorkerCapacityResponse,
        jsonMapper.writeValueAsString(indexingTotalWorkerCapacityInfo)
    );
    EasyMock.expect(druidLeaderClient.go(EasyMock.anyObject(Request.class)))
            .andReturn(autoScaleResponseHolder)
            .once();
    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.GET, "/druid/indexer/v1/totalWorkerCapacity"))
            .andReturn(new Request(
                HttpMethod.GET,
                new URL("http://localhost:8090/druid/indexer/v1/totalWorkerCapacity")
            ))
            .once();
    EasyMock.replay(druidLeaderClient);

    final int actualResponse = httpIndexingServiceClient.getTotalWorkerCapacityWithAutoScale();
    Assert.assertEquals(maximumCapacityWithAutoScale, actualResponse);
    EasyMock.verify(druidLeaderClient);
  }
}

