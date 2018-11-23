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

package org.apache.druid.server.log;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.server.RequestLogLine;

/**
 * This factory allows to customize {@link RequestLogEvent}s, emitted in {@link EmittingRequestLogger}, e. g. to exclude
 * some fields (compared to {@link DefaultRequestLogEvent}) to make the events smaller.
 *
 * The default factory creates builders that return {@link DefaultRequestLogEvent}.
 */
@PublicApi
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DefaultRequestLogEventBuilderFactory.class)
public interface RequestLogEventBuilderFactory
{
  ServiceEventBuilder<RequestLogEvent> createRequestLogEventBuilder(String feed, RequestLogLine requestLogLine);
}
