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

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

public class DirectDruidClientMetricsTest
{
  @Test
  public void testEmptyMetrics()
  {
    DirectDruidClientMetrics metrics = new DirectDruidClientMetrics();
    Assert.assertEquals(0, metrics.getErrorRate(), 0.0001);
  }

  @Test
  public void testFullMetrics()
  {
    int errorsEvery = 3;
    double sensitivity = 0.0001;
    int afterTestRequests = 100;
    int afterTestErrors = 9;
    int totalReqs = DirectDruidClientMetrics.DEFAULT_HISTORY_LENGTH;
    DirectDruidClientMetrics metrics = new DirectDruidClientMetrics();

    testMetrics(errorsEvery, sensitivity, afterTestRequests, afterTestErrors, totalReqs, metrics);
  }

  private void testMetrics(int errorsEvery, double sensitivity, int afterTestRequests, int afterTestErrors,
      int totalReqs, DirectDruidClientMetrics metrics)
  {
    simulateRequests(errorsEvery, totalReqs, metrics);
    int totalPreTestErrors = (int) Math.ceil(((double) totalReqs) / errorsEvery);
    double errorRate = ((double) totalPreTestErrors) / totalReqs;
    Assert.assertEquals(errorRate, metrics.getErrorRate(), sensitivity);
    simulateRequests(afterTestErrors, afterTestRequests, metrics);
    totalPreTestErrors -= Math.ceil((double) afterTestRequests) / errorsEvery;
    totalPreTestErrors += (int) Math.ceil(((double) afterTestRequests) / afterTestErrors);
    errorRate = ((double) totalPreTestErrors) / totalReqs;
    Assert.assertEquals(errorRate, metrics.getErrorRate(), sensitivity);
  }

  private void simulateRequests(int errorsEvery, int totalReqs, DirectDruidClientMetrics metrics)
  {
    for (int i = 0; i < totalReqs; i++) {
      if (i % errorsEvery == 0) {
        metrics.recordResponse(HttpResponseStatus.SEE_OTHER);
      } else {
        metrics.recordResponse(HttpResponseStatus.OK);
      }
    }
  }

  @Test
  public void testVariableMax()
  {
    int errorsEvery = 3;
    double sensitivity = 0.0001;
    int afterTestRequests = 100;
    int afterTestErrors = 9;
    int totalReqs = 100;
    DirectDruidClientMetrics metrics = new DirectDruidClientMetrics();
    metrics.setMaxHistoryLength(totalReqs);
    testMetrics(errorsEvery, sensitivity, afterTestRequests, afterTestErrors, totalReqs, metrics);
  }

}
