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

package org.apache.druid.indexing.overlord;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PortFinder
{
  private final Set<Integer> usedPorts = new HashSet<>();
  private final int startPort;
  private final int endPort;
  private final List<Integer> candidatePorts;

  public PortFinder(int startPort, int endPort, List<Integer> candidatePorts)
  {
    this.startPort = startPort;
    this.endPort = endPort;
    this.candidatePorts = candidatePorts;
  }

  @VisibleForTesting
  boolean canBind(int portNum)
  {
    try {
      new ServerSocket(portNum).close();
      return true;
    }
    catch (SocketException se) {
      return false;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public synchronized int findUnusedPort()
  {
    if (candidatePorts != null && !candidatePorts.isEmpty()) {
      int port = chooseFromCandidates();
      usedPorts.add(port);
      return port;
    } else {
      int port = chooseNext(startPort);
      while (!canBind(port)) {
        port = chooseNext(port + 1);
      }
      usedPorts.add(port);
      return port;
    }
  }

  public synchronized void markPortUnused(int port)
  {
    usedPorts.remove(port);
  }

  private int chooseFromCandidates()
  {
    for (int port : candidatePorts) {
      if (!usedPorts.contains(port) && canBind(port)) {
        return port;
      }
    }
    throw new ISE("All ports are used...");
  }

  private int chooseNext(int start)
  {
    // up to endPort (which default value is 65535)
    for (int i = start; i <= endPort; i++) {
      if (!usedPorts.contains(i)) {
        return i;
      }
    }
    throw new ISE("All ports are used...");
  }
}

