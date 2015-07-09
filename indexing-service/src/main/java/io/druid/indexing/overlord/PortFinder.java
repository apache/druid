/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import com.google.common.collect.Sets;
import com.metamx.common.ISE;

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

  /**
   * Force a port to be thought of as used.
   *
   * @param port The port of interest
   *
   * @return Result of Set.add
   */
  public synchronized boolean markPortUsed(int port)
  {
    return usedPorts.add(port);
  }

  /**
   * Force removing a port from the list of used ports
   *
   * @param port The port to remove
   *
   * @return The result of Set.remove
   */
  public synchronized boolean markPortUnused(int port)
  {
    return usedPorts.remove(port);
  }

  private int chooseNext(int start)
  {
    for (int i = start; i < Integer.MAX_VALUE; i++) {
      if (!usedPorts.contains(i)) {
        return i;
      }
    }
    throw new ISE("All ports are Used..");
  }
}

