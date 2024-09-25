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

package org.apache.druid.msq.dart.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.messages.client.MessageRelay;
import org.apache.druid.messages.client.MessageRelayClientImpl;
import org.apache.druid.messages.client.MessageRelayFactory;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.worker.http.DartWorkerResource;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;

/**
 * Production implementation of {@link MessageRelayFactory}.
 */
public class DartMessageRelayFactoryImpl implements MessageRelayFactory<ControllerMessage>
{
  private final String clientHost;
  private final ControllerMessageListener messageListener;
  private final ServiceClientFactory clientFactory;
  private final String basePath;
  private final ObjectMapper smileMapper;

  @Inject
  public DartMessageRelayFactoryImpl(
      @Self DruidNode selfNode,
      @EscalatedGlobal ServiceClientFactory clientFactory,
      @Smile ObjectMapper smileMapper,
      ControllerMessageListener messageListener
  )
  {
    this.clientHost = selfNode.getHostAndPortToUse();
    this.messageListener = messageListener;
    this.clientFactory = clientFactory;
    this.smileMapper = smileMapper;
    this.basePath = DartWorkerResource.PATH + "/relay";
  }

  @Override
  public MessageRelay<ControllerMessage> newRelay(DruidNode clientNode)
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
        new MessageRelayClientImpl<>(client, smileMapper, ControllerMessage.class),
        messageListener
    );
  }
}
