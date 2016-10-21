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

package io.druid.indexing.overlord;

import com.google.common.collect.Sets;

import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;

import java.io.IOException;
import java.net.BindException;
import java.net.ServerSocket;
import java.util.Set;

public class PortFinder
{
  private final Set<Integer> usedPorts = Sets.newHashSet();
  private final int startPort;

  public PortFinder(int startPort)
  {
    this.startPort = startPort;
  }

  private static boolean canBind(int portNum)
  {
    ServerSocket ss = null;
    boolean isFree = false;
    try {
      ss = new ServerSocket(portNum);
      isFree = true;
    }
    catch (BindException be) {
      isFree = false; // port in use,
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    finally {
      if (ss != null) {
        while (!ss.isClosed()) {
          try {
            ss.close();
          }
          catch (IOException e) {
            // ignore
          }
        }
      }
    }
    return isFree;
  }

  public synchronized int findUnusedPort()
  {
    int port = chooseNext(startPort);
    while (!canBind(port)) {
      port = chooseNext(port + 1);
    }
    usedPorts.add(port);
    return port;
  }

  public synchronized Pair<Integer, Integer> findTwoConsecutiveUnusedPorts()
  {
    int firstPort = chooseNext(startPort);
    while (!canBind(firstPort) || !canBind(firstPort + 1)) {
      firstPort = chooseNext(firstPort + 1);
    }
    usedPorts.add(firstPort);
    usedPorts.add(firstPort + 1);
    return new Pair<>(firstPort, firstPort + 1);
  }

  public synchronized void markPortUnused(int port)
  {
    usedPorts.remove(port);
  }

  private int chooseNext(int start)
  {
    // up to unsigned short max (65535)
    for (int i = start; i <= 0xFFFF; i++) {
      if (!usedPorts.contains(i)) {
        return i;
      }
    }
    throw new ISE("All ports are Used..");
  }
}

