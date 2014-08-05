/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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

  public synchronized void markPortUnused(int port)
  {
    usedPorts.remove(port);
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

