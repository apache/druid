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

import org.apache.druid.java.util.common.RE;
import org.apache.druid.msq.exec.ControllerClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Limits the number of exceptions that get published to the underlying delegate publisher. This helps
 * in preventing the spam of exceptions from the worker task to the published source. As such, any implementation
 * of {@link MSQWarningReportPublisher} that is wrapped in this class cannot be sure that the warning handed off
 * is trully published
 */
public class MSQWarningReportLimiterPublisher implements MSQWarningReportPublisher
{

  private final MSQWarningReportPublisher delegate;
  private final long totalLimit;
  private final Map<String, Long> errorCodeToLimit;
  private final Set<String> criticalWarningCodes;
  private final ConcurrentHashMap<String, Long> errorCodeToCurrentCount = new ConcurrentHashMap<>();
  private final ControllerClient controllerClient;
  private final String workerId;

  @Nullable
  private final String host;

  long totalCount = 0L;

  final Object lock = new Object();

  /**
   * Creates a publisher which publishes the warnings to the controller if they have not yet exceeded the allowed limit.
   * Moreover, if a warning is disallowed, i.e. it's limit is set to 0, then the publisher directly reports the warning
   * as an error
   * {@code errorCodeToLimit} refers to the maximum number of verbose warnings that should be published. The actual
   * limit for the warnings before which the controller should fail can be much higher and hence a separate {@code criticalWarningCodes}
   *
   * @param delegate The delegate publisher which publishes the allowed warnings
   * @param totalLimit Total limit of warnings that a worker can publish
   * @param errorCodeToLimit Map of error code to the number of allowed warnings that the publisher can publish
   * @param criticalWarningCodes Error codes which if encountered should be thrown as error
   * @param controllerClient Controller client (for directly sending the warning as an error)
   * @param workerId workerId, used to construct the error report
   * @param host worker' host, used to construct the error report
   */
  public MSQWarningReportLimiterPublisher(
      MSQWarningReportPublisher delegate,
      long totalLimit,
      Map<String, Long> errorCodeToLimit,
      Set<String> criticalWarningCodes,
      ControllerClient controllerClient,
      String workerId,
      @Nullable String host
  )
  {
    this.delegate = delegate;
    this.errorCodeToLimit = errorCodeToLimit;
    this.criticalWarningCodes = criticalWarningCodes;
    this.totalLimit = totalLimit;
    this.controllerClient = controllerClient;
    this.workerId = workerId;
    this.host = host;
  }

  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    String errorCode = MSQErrorReport.getFaultFromException(e).getErrorCode();
    synchronized (lock) {
      totalCount = totalCount + 1;
      errorCodeToCurrentCount.compute(errorCode, (ignored, count) -> count == null ? 1L : count + 1);

      // Send the warning as an error if it is disallowed altogether
      if (criticalWarningCodes.contains(errorCode)) {
        try {
          controllerClient.postWorkerError(workerId, MSQErrorReport.fromException(workerId, host, stageNumber, e));
        }
        catch (IOException postException) {
          throw new RE(postException, "Failed to post the worker error [%s] to the controller", errorCode);
        }
      }

      if (totalLimit != -1 && totalCount > totalLimit) {
        return;
      }
    }

    long limitForFault = errorCodeToLimit.getOrDefault(errorCode, -1L);
    synchronized (lock) {
      if (limitForFault != -1 && errorCodeToCurrentCount.getOrDefault(errorCode, 0L) > limitForFault) {
        return;
      }
    }
    delegate.publishException(stageNumber, e);
  }

  @Override
  public void close() throws IOException
  {
    delegate.close();
  }
}
