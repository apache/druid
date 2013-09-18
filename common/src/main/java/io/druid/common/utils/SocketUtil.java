/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.common.utils;

import com.metamx.common.ISE;

import java.io.IOException;
import java.net.ServerSocket;

/**
 */
public class SocketUtil
{
  public static int findOpenPort(int startPort)
  {
    int currPort = startPort;

    while (currPort < 0xffff) {
      ServerSocket socket = null;
      try {
        socket = new ServerSocket(currPort);
        return currPort;
      }
      catch (IOException e) {
        ++currPort;
      }
      finally {
        if (socket != null) {
          try {
            socket.close();
          }
          catch (IOException e) {

          }
        }
      }
    }

    throw new ISE("Unable to find open port between[%d] and [%d]", startPort, currPort);
  }
}
