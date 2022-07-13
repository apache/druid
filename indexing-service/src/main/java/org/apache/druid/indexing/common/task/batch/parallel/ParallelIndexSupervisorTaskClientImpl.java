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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.rpc.IgnoreHttpResponseHandler;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;

public class ParallelIndexSupervisorTaskClientImpl implements ParallelIndexSupervisorTaskClient
{
  private final ServiceClient client;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final Duration httpTimeout;

  public ParallelIndexSupervisorTaskClientImpl(
      ServiceClient client,
      ObjectMapper jsonMapper,
      ObjectMapper smileMapper,
      Duration httpTimeout
  )
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.httpTimeout = httpTimeout;
  }

  @Override
  public SegmentIdWithShardSpec allocateSegment(DateTime timestamp) throws IOException
  {
    // API accepts Smile requests and sends JSON responses.
    return jsonMapper.readValue(
        FutureUtils.getUnchecked(
            client.asyncRequest(
                new RequestBuilder(HttpMethod.POST, "/segment/allocate")
                    .smileContent(smileMapper, timestamp)
                    .timeout(httpTimeout),
                new BytesFullResponseHandler()
            ),
            true
        ).getContent(),
        SegmentIdWithShardSpec.class
    );
  }

  @Override
  public SegmentIdWithShardSpec allocateSegment(
      DateTime timestamp,
      String sequenceName,
      @Nullable String prevSegmentId
  ) throws IOException
  {
    // API accepts Smile requests and sends JSON responses.
    return jsonMapper.readValue(
        FutureUtils.getUnchecked(
            client.asyncRequest(
                new RequestBuilder(HttpMethod.POST, "/segment/allocate")
                    .smileContent(smileMapper, new SegmentAllocationRequest(timestamp, sequenceName, prevSegmentId))
                    .timeout(httpTimeout),
                new BytesFullResponseHandler()
            ),
            true
        ).getContent(),
        SegmentIdWithShardSpec.class
    );
  }

  @Override
  public void report(SubTaskReport report)
  {
    FutureUtils.getUnchecked(
        client.asyncRequest(
            new RequestBuilder(HttpMethod.POST, "/report")
                .smileContent(smileMapper, report)
                .timeout(httpTimeout),
            IgnoreHttpResponseHandler.INSTANCE
        ),
        true
    );
  }
}
