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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;

/**
 * Production implementation of {@link MessageRelayFactory}.
 */
public class MessageRelayFactoryImpl<MessageType> implements MessageRelayFactory<MessageType>
{
  private final String clientHost;
  private final MessageListener<MessageType> messageListener;
  private final ServiceClientFactory clientFactory;
  private final String basePath;
  private final ObjectMapper smileMapper;
  private final Class<MessageType> messageClass;

  public MessageRelayFactoryImpl(
      final String clientHost,
      final MessageListener<MessageType> messageListener,
      final ServiceClientFactory clientFactory,
      final String basePath,
      final ObjectMapper smileMapper,
      final Class<MessageType> messageClass
  )
  {
    this.clientHost = clientHost;
    this.messageListener = messageListener;
    this.clientFactory = clientFactory;
    this.smileMapper = smileMapper;
    this.messageClass = messageClass;
    this.basePath = basePath;
  }

  @Override
  public MessageRelay<MessageType> newRelay(DruidNode clientNode)
  {
    final ServiceLocation location = ServiceLocation.fromDruidNode(clientNode).withBasePath(basePath);
    final ServiceClient client = clientFactory.makeClient(
        clientNode.getHostAndPortToUse(),
        new FixedServiceLocator(location),
        StandardRetryPolicy.unlimited()
    );

    return new MessageRelay<>(
        clientHost,
        clientNode,
        new MessageRelayClientImpl<>(client, smileMapper, messageClass),
        messageListener
    );
  }
}
