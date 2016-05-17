/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.server.coordinator;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.concurrent.Executors;

public class CostBalancerStrategyFactory implements BalancerStrategyFactory
{
  private final ListeningExecutorService exec;

  public CostBalancerStrategyFactory(int costBalancerStrategyThreadCount)
  {
    this.exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(costBalancerStrategyThreadCount));
  }

  @Override
  public CostBalancerStrategy createBalancerStrategy(DateTime referenceTimestamp)
  {
    return new CostBalancerStrategy(exec);
  }

  @Override
  public void close() throws IOException
  {
    exec.shutdownNow();
  }
}
