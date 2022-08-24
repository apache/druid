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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.exec.Limits;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Limits the number of exceptions that get published to the underlying delegate publisher. This helps
 * in preventing the spam of exceptions from the worker task to the published source. As such, any implementation
 * of {@link MSQWarningReportPublisher} that is wrapped in this class cannot be sure that the warning handed off
 * is trully published
 */
public class MSQWarningReportLimiterPublisher implements MSQWarningReportPublisher
{

  final MSQWarningReportPublisher delegate;
  final long totalLimit;
  final Map<String, Long> errorCodeToLimit;
  final ConcurrentHashMap<String, Long> errorCodeToCurrentCount = new ConcurrentHashMap<>();

  long totalCount = 0L;

  final Object lock = new Object();

  public MSQWarningReportLimiterPublisher(MSQWarningReportPublisher delegate)
  {
    this(
        delegate,
        Limits.MAX_VERBOSE_WARNINGS,
        ImmutableMap.of(
            CannotParseExternalDataFault.CODE, Limits.MAX_VERBOSE_PARSE_EXCEPTIONS
        )
    );
  }

  public MSQWarningReportLimiterPublisher(
      MSQWarningReportPublisher delegate,
      long totalLimit,
      Map<String, Long> errorCodeToLimit
  )
  {
    this.delegate = delegate;
    this.errorCodeToLimit = errorCodeToLimit;
    this.totalLimit = totalLimit;
  }

  @Override
  public void publishException(int stageNumber, Throwable e)
  {
    String errorCode = MSQErrorReport.getFaultFromException(e).getErrorCode();
    synchronized (lock) {
      totalCount = totalCount + 1;
      errorCodeToCurrentCount.compute(errorCode, (ignored, count) -> count == null ? 1L : count + 1);

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
