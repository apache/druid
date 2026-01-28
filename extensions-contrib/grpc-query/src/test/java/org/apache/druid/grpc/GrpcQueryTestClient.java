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

package org.apache.druid.grpc;

import org.apache.druid.grpc.proto.QueryOuterClass.ColumnSchema;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryRequest;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResponse;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryResultFormat;
import org.apache.druid.grpc.proto.QueryOuterClass.QueryStatus;
import org.apache.druid.java.util.common.StringUtils;

import java.nio.charset.StandardCharsets;

/**
 * Super-simple command-line (or IDE launched) client which makes a
 * single query request and prints
 * the response. Useful because Druid provides no other rRPC client
 * to use to test the rRPC endpoint. Pass the desired query as the
 * one and only command line parameter. Does not (yet) support the
 * query context or query parameters.
 */
public class GrpcQueryTestClient
{
  public static void main(String[] args)
  {
    if (args.length != 1) {
      System.err.println("Usage: sql-query");
      System.exit(1);
    }
    TestClient client = new TestClient(TestClient.DEFAULT_HOST);
    QueryRequest request = QueryRequest.newBuilder()
                                       .setQuery(args[0])
                                       .setResultFormat(QueryResultFormat.CSV)
                                       .build();
    QueryResponse response = client.getQueryClient().submitQuery(request);


    if (response.getStatus() != QueryStatus.OK) {
      System.err.println("Failed: " + response.getStatus().name());
      System.err.println(response.getErrorMessage());
      System.exit(1);
    }
    System.out.println("Columns:");
    for (ColumnSchema col : response.getColumnsList()) {
      System.out.println(StringUtils.format("%s %s (%s)", col.getName(), col.getSqlType(), col.getDruidType().name()));
    }
    System.out.println("Data:");
    System.out.println(response.getData().toString(StandardCharsets.UTF_8));
    client.close();
  }
}
