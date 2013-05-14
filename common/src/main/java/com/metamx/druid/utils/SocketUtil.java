package com.metamx.druid.utils;

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
