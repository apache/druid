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

package org.apache.druid.grpc.client;

import com.google.protobuf.ByteString;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.TestResults.QueryResult;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GrpcResponseHandlerTest
{
  private static List<QueryResult> EXPECTED_RESULTS = Arrays.asList(
      QueryResult.newBuilder().setDim1("test").setCnt(100).build(),
      QueryResult.newBuilder().setDim2("test2").setCnt(100).setM2(200.10).build()
  );

  @Test
  public void testEmptyResponse()
  {
    GrpcResponseHandler<QueryResult> handler = GrpcResponseHandler.of(QueryResult.class);
    List<QueryResult> queryResults = handler.get(ByteString.EMPTY);
    assertTrue(queryResults.isEmpty());
  }

  @Test
  public void testNonEmptyResponse()
  {
    GrpcResponseHandler<QueryResult> handler = GrpcResponseHandler.of(QueryResult.class);
    QueryResponse queryResponse = getQueryResponse();
    List<QueryResult> queryResults = handler.get(queryResponse.getData());
    assertEquals(2, queryResults.size());
    assertEquals(EXPECTED_RESULTS, queryResults);
  }

  private static QueryResponse getQueryResponse()
  {
    try {
      final ByteArrayOutputStream out = new ByteArrayOutputStream();
      for (QueryResult queryResult : EXPECTED_RESULTS) {
        queryResult.writeDelimitedTo(out);
      }
      return QueryResponse.newBuilder()
                          .setData(ByteString.copyFrom(out.toByteArray()))
                          .build();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
