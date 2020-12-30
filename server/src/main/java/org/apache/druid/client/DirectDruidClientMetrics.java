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

package org.apache.druid.client;

import org.apache.druid.java.util.common.Pair;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class DirectDruidClientMetrics
{
  public static final int DEFAULT_HISTORY_LENGTH = 500;

  private final AtomicInteger openConnections;

  private final ConcurrentLinkedDeque<Pair<Long, HttpResponseStatus>> responseHistory;

  private final AtomicInteger errorCount;

  // ConcurrentLinkedDeque size method is not constant time, so maintain this
  private final AtomicInteger historySize;

  private int maxHistoryLength = DEFAULT_HISTORY_LENGTH;

  public DirectDruidClientMetrics()
  {
    this.openConnections = new AtomicInteger();
    this.errorCount = new AtomicInteger();
    this.historySize = new AtomicInteger();
    this.responseHistory = new ConcurrentLinkedDeque<>();
  }

  public int openConnection()
  {
    return openConnections.getAndIncrement();
  }

  public int closeConnection()
  {
    return openConnections.getAndDecrement();
  }

  public int getOpenConnections()
  {
    return openConnections.get();
  }

  public double getErrorRate()
  {
    int size = historySize.get();

    if (size == 0) {
      return 0;
    }
    
    return ((double) errorCount.get()) / ((double) size);
  }

  public ConcurrentLinkedDeque<Pair<Long, HttpResponseStatus>> getResponseHistory()
  {
    return responseHistory;
  }

  public AtomicInteger getErrorCount()
  {
    return errorCount;
  }

  public void recordResponse(HttpResponseStatus status)
  {
    while (responseHistory.size() >= maxHistoryLength) {
      if (!isSuccess(responseHistory.poll().rhs)) {
        errorCount.decrementAndGet();
      }
      historySize.decrementAndGet();
    }
    responseHistory.add(Pair.of(System.currentTimeMillis(), status));
    if (!isSuccess(status)) {
      errorCount.incrementAndGet();
    }
    historySize.incrementAndGet();
  }

  private boolean isSuccess(HttpResponseStatus status)
  {
    return HttpResponseStatus.OK.equals(status) ||
        HttpResponseStatus.CREATED.equals(status) ||
        HttpResponseStatus.ACCEPTED.equals(status);
  }

  public int getMaxHistoryLength()
  {
    return maxHistoryLength;
  }

  public void setMaxHistoryLength(int maxHistoryLength)
  {
    this.maxHistoryLength = maxHistoryLength;
  }
}
