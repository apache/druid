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

package org.apache.druid.msq.rpc;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;

/**
 * Production implementation of {@link CoordinatorServiceClient}.
 */
public class CoordinatorServiceClientImpl implements CoordinatorServiceClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;

  public CoordinatorServiceClientImpl(final ServiceClient client, final ObjectMapper jsonMapper)
  {
    this.client = Preconditions.checkNotNull(client, "client");
    this.jsonMapper = Preconditions.checkNotNull(jsonMapper, "jsonMapper");
  }

  @Override
  public ListenableFuture<DataSegment> fetchUsedSegment(String dataSource, String segmentId)
  {
    final String path = StringUtils.format(
        "/druid/coordinator/v1/metadata/datasources/%s/segments/%s",
        StringUtils.urlEncode(dataSource),
        StringUtils.urlEncode(segmentId)
    );

    return FutureUtils.transform(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.GET, path),
            new BytesFullResponseHandler()
        ),
        holder -> deserialize(holder, new TypeReference<DataSegment>() {})
    );
  }

  @Override
  public CoordinatorServiceClient withRetryPolicy(ServiceRetryPolicy retryPolicy)
  {
    return new CoordinatorServiceClientImpl(client.withRetryPolicy(retryPolicy), jsonMapper);
  }

  /**
   * Deserialize a {@link BytesFullResponseHolder} as JSON.
   *
   * It would be reasonable to move this to {@link BytesFullResponseHolder} itself, or some shared utility class.
   */
  private <T> T deserialize(final BytesFullResponseHolder bytesHolder, final TypeReference<T> typeReference)
  {
    try {
      return jsonMapper.readValue(bytesHolder.getContent(), typeReference);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
