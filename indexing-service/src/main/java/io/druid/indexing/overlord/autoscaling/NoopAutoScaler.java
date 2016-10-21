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

package io.druid.indexing.overlord.autoscaling;

import com.metamx.emitter.EmittingLogger;

import io.druid.java.util.common.UOE;

import java.util.List;

/**
 * This class just logs when scaling should occur.
 */
public class NoopAutoScaler<Void> implements AutoScaler<Void>
{
  private static final EmittingLogger log = new EmittingLogger(NoopAutoScaler.class);

  @Override
  public int getMinNumWorkers()
  {
    return 0;
  }

  @Override
  public int getMaxNumWorkers()
  {
    return 0;
  }

  @Override
  public Void getEnvConfig()
  {
    throw new UOE("No config for Noop!");
  }

  @Override
  public AutoScalingData provision()
  {
    log.info("If I were a real strategy I'd create something now");
    return null;
  }

  @Override
  public AutoScalingData terminate(List<String> ips)
  {
    log.info("If I were a real strategy I'd terminate %s now", ips);
    return null;
  }

  @Override
  public AutoScalingData terminateWithIds(List<String> ids)
  {
    log.info("If I were a real strategy I'd terminate %s now", ids);
    return null;
  }

  @Override
  public List<String> ipToIdLookup(List<String> ips)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s", ips);
    return ips;
  }

  @Override
  public List<String> idToIpLookup(List<String> nodeIds)
  {
    log.info("I'm not a real strategy so I'm returning what I got %s", nodeIds);
    return nodeIds;
  }
}
