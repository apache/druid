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

package org.apache.druid.msq.dart.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.exec.MemoryIntrospector;
import org.apache.druid.msq.exec.ProcessingBuffersProvider;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.input.table.DataSegmentProvider;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.server.DruidNode;

import java.io.File;

/**
 * Production implementation of {@link DartWorkerContextFactory}.
 */
public class DartWorkerContextFactoryImpl implements DartWorkerContextFactory
{
  private final DruidNode selfNode;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final PolicyEnforcer policyEnforcer;
  private final Injector injector;
  private final ServiceClientFactory serviceClientFactory;
  private final DruidProcessingConfig processingConfig;
  private final SegmentWrangler segmentWrangler;
  private final GroupingEngine groupingEngine;
  private final DataSegmentProvider dataSegmentProvider;
  private final MemoryIntrospector memoryIntrospector;
  private final ProcessingBuffersProvider processingBuffersProvider;
  private final Outbox<ControllerMessage> outbox;
  private final DartDataServerQueryHandlerFactory dataServerQueryHandlerFactory;
  private final ServiceEmitter emitter;

  @Inject
  public DartWorkerContextFactoryImpl(
      @Self DruidNode selfNode,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      PolicyEnforcer policyEnforcer,
      Injector injector,
      @EscalatedGlobal ServiceClientFactory serviceClientFactory,
      DruidProcessingConfig processingConfig,
      SegmentWrangler segmentWrangler,
      GroupingEngine groupingEngine,
      @Dart DataSegmentProvider dataSegmentProvider,
      MemoryIntrospector memoryIntrospector,
      @Dart ProcessingBuffersProvider processingBuffersProvider,
      Outbox<ControllerMessage> outbox,
      DartDataServerQueryHandlerFactory dataServerQueryHandlerFactory,
      ServiceEmitter emitter
  )
  {
    this.selfNode = selfNode;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.policyEnforcer = policyEnforcer;
    this.injector = injector;
    this.serviceClientFactory = serviceClientFactory;
    this.processingConfig = processingConfig;
    this.segmentWrangler = segmentWrangler;
    this.groupingEngine = groupingEngine;
    this.dataSegmentProvider = dataSegmentProvider;
    this.memoryIntrospector = memoryIntrospector;
    this.processingBuffersProvider = processingBuffersProvider;
    this.outbox = outbox;
    this.dataServerQueryHandlerFactory = dataServerQueryHandlerFactory;
    this.emitter = emitter;
  }

  @Override
  public WorkerContext build(
      String queryId,
      String controllerHost,
      File tempDir,
      QueryContext queryContext
  )
  {
    return new DartWorkerContext(
        queryId,
        controllerHost,
        selfNode,
        jsonMapper,
        policyEnforcer,
        injector,
        new DartWorkerClientImpl(queryId, serviceClientFactory, smileMapper, null),
        processingConfig,
        segmentWrangler,
        groupingEngine,
        dataSegmentProvider,
        memoryIntrospector,
        processingBuffersProvider,
        outbox,
        tempDir,
        queryContext,
        dataServerQueryHandlerFactory,
        emitter
    );
  }
}
