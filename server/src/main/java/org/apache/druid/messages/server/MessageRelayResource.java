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

package org.apache.druid.messages.server;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.messages.MessageBatch;
import org.apache.druid.messages.client.MessageListener;
import org.apache.druid.messages.client.MessageRelayClient;
import org.apache.druid.server.http.AsyncRequest;
import org.apache.druid.server.http.AsyncRequest.AsyncResponse;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;

/**
 * Server-side resource for message relaying. Wraps an {@link Outbox} and {@link MessageListener}.
 * The client for this resource is {@link MessageRelayClient}.
 */
public class MessageRelayResource<MessageType>
{
  private static final Logger log = new Logger(MessageRelayResource.class);
  private static final long GET_MESSAGES_TIMEOUT = 30_000L;

  /**
   * Outbox for messages sent from this server.
   */
  private final Outbox<MessageType> outbox;

  /**
   * Message relay protocol uses Smile.
   */
  private final ObjectMapper smileMapper;

  /**
   * Type of {@link MessageBatch} of {@link MessageType}.
   */
  private final JavaType batchType;

  public MessageRelayResource(
      final Outbox<MessageType> outbox,
      final ObjectMapper smileMapper,
      final Class<MessageType> messageClass
  )
  {
    this.outbox = outbox;
    this.smileMapper = smileMapper;
    this.batchType = smileMapper.getTypeFactory().constructParametricType(MessageBatch.class, messageClass);
  }

  /**
   * Retrieve messages from the outbox for a particular client, as a {@link MessageBatch} in Smile format.
   * The messages are retrieved from {@link Outbox#getMessages(String, long, long)}.
   *
   * This is a long-polling async method, using {@link AsyncRequest} to wait up to {@link #GET_MESSAGES_TIMEOUT} for
   * messages to appear in the outbox.
   *
   * @return HTTP 200 with Smile response with messages on success; HTTP 204 (No Content) if no messages were put in
   * the outbox before the timeout {@link #GET_MESSAGES_TIMEOUT} elapsed
   *
   * @see Outbox#getMessages(String, long, long) for more details on the API
   */
  @GET
  @Path("/outbox/{clientHost}/messages")
  public Void httpGetMessagesFromOutbox(
      @PathParam("clientHost") final String clientHost,
      @QueryParam("epoch") final Long epoch,
      @QueryParam("watermark") final Long watermark,
      @Context final HttpServletRequest req
  )
  {
    if (epoch == null || watermark == null || clientHost == null || clientHost.isEmpty()) {
      AsyncRequest.completed(
          req,
          smileMapper,
          SmileMediaTypes.APPLICATION_JACKSON_SMILE,
          AsyncResponse.error(HttpServletResponse.SC_BAD_REQUEST, null)
      );
      return null;
    }

    final ListenableFuture<MessageBatch<MessageType>> batchFuture = outbox.getMessages(clientHost, epoch, watermark);

    // Save these items, since "req" becomes inaccessible in future exception handlers.
    final String remoteAddr = req.getRemoteAddr();
    final String requestURI = req.getRequestURI();

    AsyncRequest.create(req, smileMapper, SmileMediaTypes.APPLICATION_JACKSON_SMILE, batchFuture, GET_MESSAGES_TIMEOUT)
                .onTimeout(HttpServletResponse.SC_NO_CONTENT)
                .onSuccess(
                    result -> {
                      log.debug("Sending message batch: %s", result);
                      return AsyncResponse.of(HttpServletResponse.SC_OK, result, batchType);
                    }
                )
                .onFailure(e -> {
                  log.noStackTrace().warn(e, "Request failed from[%s] to[%s]", remoteAddr, requestURI);
                  return AsyncResponse.error(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, null);
                })
                .start();

    return null;
  }
}
