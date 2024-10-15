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

package org.apache.druid.messages.client;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHandler;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.eclipse.jetty.http.HttpStatus;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.Collections;

/**
 * Production implementation of {@link MessageRelayClient}.
 */
public class MessageRelayClientImpl<MessageType> implements MessageRelayClient<MessageType>
{
  private final ServiceClient serviceClient;
  private final ObjectMapper smileMapper;
  private final JavaType inMessageBatchType;

  public MessageRelayClientImpl(
      final ServiceClient serviceClient,
      final ObjectMapper smileMapper,
      final Class<MessageType> inMessageClass
  )
  {
    this.serviceClient = serviceClient;
    this.smileMapper = smileMapper;
    this.inMessageBatchType = smileMapper.getTypeFactory().constructParametricType(MessageBatch.class, inMessageClass);
  }

  @Override
  public ListenableFuture<MessageBatch<MessageType>> getMessages(
      final String clientHost,
      final long epoch,
      final long startWatermark
  )
  {
    final String path = StringUtils.format(
        "/outbox/%s/messages?epoch=%d&watermark=%d",
        StringUtils.urlEncode(clientHost),
        epoch,
        startWatermark
    );

    ListenableFuture<BytesFullResponseHolder> asyncRequest = serviceClient.asyncRequest(
        new RequestBuilder(HttpMethod.GET, path),
        new BytesFullResponseHandler()
    );
    return FutureUtils.transform(
        asyncRequest,
        holder -> {
          if (holder.getResponse().getStatus().getCode() == HttpStatus.NO_CONTENT_204) {
            return new MessageBatch<>(Collections.emptyList(), epoch, startWatermark);
          } else {
            return JacksonUtils.readValue(smileMapper, holder.getContent(), inMessageBatchType);
          }
        }
    );
  }
}
