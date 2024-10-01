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

package org.apache.druid.msq.dart.controller.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.dart.controller.http.DartSqlResource;
import org.apache.druid.rpc.FixedServiceLocator;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.DruidNode;

/**
 * Production implementation of {@link DartSqlClientFactory}.
 */
public class DartSqlClientFactoryImpl implements DartSqlClientFactory
{
  private final ServiceClientFactory clientFactory;
  private final ObjectMapper jsonMapper;

  @Inject
  public DartSqlClientFactoryImpl(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.clientFactory = clientFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public DartSqlClient makeClient(DruidNode node)
  {
    final ServiceClient client = clientFactory.makeClient(
        StringUtils.format("%s[dart-sql]", node.getHostAndPortToUse()),
        new FixedServiceLocator(ServiceLocation.fromDruidNode(node).withBasePath(DartSqlResource.PATH)),
        StandardRetryPolicy.noRetries()
    );

    return new DartSqlClientImpl(client, jsonMapper);
  }
}
