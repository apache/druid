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

package io.druid.indexing.overlord;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.ServerSocket;

public class PortFinderTest
{
  private PortFinder finderUseStartPort;
  private PortFinder finderUseCandidatePorts;

  @Before
  public void setUp()
  {
    finderUseStartPort = new PortFinder(1200, ImmutableList.of());
    // find two unused port for 'finderUseCandidatePorts'
    for (int i = 1024; i <= 0xFFFF; ++i) {
      try {
        new ServerSocket(i).close();
        new ServerSocket(i + 1).close();
        finderUseCandidatePorts = new PortFinder(1200, ImmutableList.of(i, i + 1));
        break;
      }
      catch (Exception e) {
        // do nothing
      }
    }
  }

  @Test
  public void testUseStartPort() throws IOException
  {
    final int port1 = finderUseStartPort.findUnusedPort();
    // verify that the port is free
    ServerSocket socket1 = new ServerSocket(port1);
    finderUseStartPort.markPortUnused(port1);
    final int port2 = finderUseStartPort.findUnusedPort();
    Assert.assertNotEquals("Used port is not reallocated", port1, port2);
    // verify that port2 is free
    ServerSocket socket2 = new ServerSocket(port2);

    socket1.close();
    // Now port1 should get recycled
    Assert.assertEquals(port1, finderUseStartPort.findUnusedPort());

    socket2.close();
    finderUseStartPort.markPortUnused(port1);
    finderUseStartPort.markPortUnused(port2);
  }

  @Test
  public void testUseCandidatePorts() throws IOException
  {
    final int port1 = finderUseCandidatePorts.findUnusedPort();
    // verify that the port is free
    ServerSocket socket1 = new ServerSocket(port1);
    finderUseCandidatePorts.markPortUnused(port1);
    final int port2 = finderUseCandidatePorts.findUnusedPort();
    Assert.assertNotEquals("Used port is not reallocated", port1, port2);
    // verify that port2 is free
    ServerSocket socket2 = new ServerSocket(port2);

    socket1.close();
    // Now port1 should get recycled
    Assert.assertEquals(port1, finderUseCandidatePorts.findUnusedPort());

    socket2.close();
    finderUseCandidatePorts.markPortUnused(port1);
    finderUseCandidatePorts.markPortUnused(port2);
  }
}
