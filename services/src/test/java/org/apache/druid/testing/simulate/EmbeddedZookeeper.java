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

package org.apache.druid.testing.simulate;

import org.apache.druid.curator.CuratorTestBase;

/**
 * {@link EmbeddedResource} for an embedded zookeeper cluster that can be used
 * as a JUnit Rule in simulation tests.
 *
 * @see EmbeddedDruidCluster
 */
public class EmbeddedZookeeper implements EmbeddedResource
{
  private final CuratorTestBase zk = new CuratorTestBase();

  @Override
  public void before() throws Exception
  {
    zk.setupServerAndCurator();
  }

  @Override
  public void after()
  {
    zk.tearDownServerAndCurator();
  }

  /**
   * Connection string for this embedded Zookeeper.
   * @return A valid Zookeeper string only after {@link #before()} has been called.
   */
  public String getConnectString()
  {
    return zk.getConnectString();
  }
}
