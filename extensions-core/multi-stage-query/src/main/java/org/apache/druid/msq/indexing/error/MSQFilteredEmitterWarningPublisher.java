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

package org.apache.druid.msq.indexing.error;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceLogEvent;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

public class MSQFilteredEmitterWarningPublisher implements MSQWarningPublisher
{

  private final String workerId;
  private final String controllerTaskId;
  private final String taskId;
  @Nullable
  private final String host;
  private final ServiceEmitter emitter;
  private final Set<String> acceptableErrorCode;

  public MSQFilteredEmitterWarningPublisher(
      final String workerId,
      final String controllerTaskId,
      final String taskId,
      @Nullable final String host,
      ServiceEmitter emitter,
      Set<String> acceptableErrorCode
  )
  {
    this.workerId = workerId;
    this.controllerTaskId = controllerTaskId;
    this.taskId = taskId;
    this.host = host;
    this.emitter = emitter;
    this.acceptableErrorCode = acceptableErrorCode;
  }

  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    String errorCode = MSQErrorReport.getFaultFromException(e).getErrorCode();
    if (acceptableErrorCode.contains(errorCode)) {
      MSQErrorReport errorReport = MSQErrorReport.fromException(workerId, host, stageNumber, e);
      String errorMessage = errorReport.getFault().getErrorMessage();
      ServiceLogEvent.Builder serviceEventBuilder = ServiceLogEvent.builder();
      serviceEventBuilder.setFeed("serviceEvent");
      serviceEventBuilder.setDimension("taskId", taskId);
      serviceEventBuilder.setDimension("groupId", controllerTaskId);
      if (host != null) {
        serviceEventBuilder.setDimension("host", host);
      }
      serviceEventBuilder.setDimension("workerId", workerId);
      serviceEventBuilder.setDimension("message", errorMessage);
      serviceEventBuilder.setDimension("msqErrorCode", errorCode);
      // todo get actual error datetime from event?
      emitter.emit(serviceEventBuilder.build(DateTime.now(DateTimeZone.UTC)));
    }
  }

  @Override
  public void close() throws IOException
  {

  }
}
