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

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.server.RequestLogLine;

public class EmittingRequestLogger implements RequestLogger
{
  private final ServiceEmitter emitter;
  private final String feed;
  private final RequestLogEventBuilderFactory requestLogEventBuilderFactory;
  private final boolean logSegmentMetadataQueries;

  EmittingRequestLogger(
      ServiceEmitter emitter,
      String feed,
      RequestLogEventBuilderFactory requestLogEventBuilderFactory,
      boolean logSegmentMetadataQueries
  )
  {
    this.emitter = emitter;
    this.feed = feed;
    this.requestLogEventBuilderFactory = requestLogEventBuilderFactory;
    this.logSegmentMetadataQueries = logSegmentMetadataQueries;
  }

  @Override
  public void logNativeQuery(RequestLogLine requestLogLine)
  {
    final Query query = requestLogLine.getQuery();
    if (query != null && !logSegmentMetadataQueries && query.getType().equals(Query.SEGMENT_METADATA)) {
      return;
    }

    emitter.emit(requestLogEventBuilderFactory.createRequestLogEventBuilder(feed, requestLogLine));
  }

  @Override
  public void logSqlQuery(RequestLogLine requestLogLine)
  {
    final Query query = requestLogLine.getQuery();
    if (query != null && !logSegmentMetadataQueries && query.getType().equals(Query.SEGMENT_METADATA)) {
      return;
    }
    emitter.emit(requestLogEventBuilderFactory.createRequestLogEventBuilder(feed, requestLogLine));
  }

  @Override
  public String toString()
  {
    return "EmittingRequestLogger{" +
           "emitter=" + emitter +
           ", feed='" + feed + '\'' +
           ", requestLogEventBuilderFactory=" + requestLogEventBuilderFactory +
           ", logSegmentMetadataQueries=" + logSegmentMetadataQueries +
           '}';
  }
}
