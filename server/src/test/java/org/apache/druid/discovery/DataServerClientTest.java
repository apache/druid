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

package org.apache.druid.discovery;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.DefaultResponseContext;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.MultipleSpecificSegmentSpec;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import java.util.Collections;

import static org.apache.druid.query.Druids.newScanQueryBuilder;
import static org.mockito.Mockito.mock;

public class DataServerClientTest
{
  MockServiceClient serviceClient;
  ServiceClientFactory serviceClientFactory;
  ObjectMapper jsonMapper;
  ScanQuery query;
  DataServerClient target;

  @Before
  public void setUp()
  {
    jsonMapper = DruidServiceTestUtils.newJsonMapper();
    serviceClient = new MockServiceClient();
    serviceClientFactory = (serviceName, serviceLocator, retryPolicy) -> serviceClient;

    query = newScanQueryBuilder()
      .dataSource("dataSource1")
      .intervals(
          new MultipleSpecificSegmentSpec(
              ImmutableList.of(
                  new SegmentDescriptor(Intervals.of("2003/2004"), "v0", 1)
              )
          )
      )
      .columns("__time", "cnt", "dim1", "dim2", "m1", "m2", "unique_dim1")
      .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
      .build();

    target = new DataServerClient(
        serviceClientFactory,
        mock(ServiceLocation.class),
        jsonMapper,
        Execs.scheduledSingleThreaded("query-cancellation-executor")
    );
  }

  @Test
  public void testFetchSegmentFromDataServer() throws JsonProcessingException
  {
    ScanResultValue scanResultValue = new ScanResultValue(
        null,
        ImmutableList.of("id", "name"),
        ImmutableList.of(
            ImmutableList.of(1, "abc"),
            ImmutableList.of(5, "efg")
        ));

    RequestBuilder requestBuilder = new RequestBuilder(HttpMethod.POST, "/druid/v2/")
        .jsonContent(jsonMapper, query);
    serviceClient.expectAndRespond(
        requestBuilder,
        HttpResponseStatus.OK,
        ImmutableMap.of(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON),
        jsonMapper.writeValueAsBytes(Collections.singletonList(scanResultValue))
    );

    ResponseContext responseContext = new DefaultResponseContext();
    Sequence<ScanResultValue> result = target.run(
        query,
        responseContext,
        jsonMapper.getTypeFactory().constructType(ScanResultValue.class),
        Closer.create()
    );

    Assert.assertEquals(ImmutableList.of(scanResultValue), result.toList());
  }
}
