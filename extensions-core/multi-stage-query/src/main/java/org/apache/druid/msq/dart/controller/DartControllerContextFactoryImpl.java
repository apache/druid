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
import com.google.inject.Injector;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.msq.dart.worker.DartWorkerClientImpl;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.DruidNode;

public class DartControllerContextFactoryImpl implements DartControllerContextFactory
{
  protected final Injector injector;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final DruidNode selfNode;
  protected final ServiceClientFactory serviceClientFactory;
  protected final TimelineServerView serverView;
  protected final MemoryIntrospector memoryIntrospector;
  protected final ServiceEmitter emitter;

  @Inject
  public DartControllerContextFactoryImpl(
      final Injector injector,
      @Json final ObjectMapper jsonMapper,
      @Smile final ObjectMapper smileMapper,
      @Self final DruidNode selfNode,
      @EscalatedGlobal final ServiceClientFactory serviceClientFactory,
      final MemoryIntrospector memoryIntrospector,
      final TimelineServerView serverView,
      final ServiceEmitter emitter
  )
  {
    this.injector = injector;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.selfNode = selfNode;
    this.serviceClientFactory = serviceClientFactory;
    this.serverView = serverView;
    this.memoryIntrospector = memoryIntrospector;
    this.emitter = emitter;
  }

  @Override
  public ControllerContext newContext(final String queryId)
  {
    return new DartControllerContext(
        injector,
        jsonMapper,
        selfNode,
        new DartWorkerClientImpl(queryId, serviceClientFactory, smileMapper, selfNode.getHostAndPortToUse()),
        memoryIntrospector,
        serverView,
        emitter
    );
  }
}
