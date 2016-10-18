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

package io.druid.common.utils;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class SocketUtil
{
  public static int findOpenPort(int startPort)
  {
    int currPort = startPort;
    final byte[] key = new byte[1<<10];
    final Random rnd = new Random(473819178L);
    rnd.nextBytes(key);

    while (currPort < 0xffff) {
      final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor());
      try(final ServerSocket serverSocket =  new ServerSocket(currPort)){
        final ListenableFuture<?> future = executorService
            .submit(
                new Runnable()
                {
                  @Override
                  public void run()
                  {
                    try (Socket socket = serverSocket.accept()) {
                      try (InputStream inputStream = socket.getInputStream()) {
                        final byte[] check = new byte[key.length];
                        try {
                          int off = 0;
                          while (off < check.length) {
                            int i = inputStream.read(check, off, check.length - off);
                            if(i < 0){
                              throw new IOException("EOS");
                            }
                            off += i;
                          }
                        }
                        catch (IOException e) {
                          throw Throwables.propagate(e);
                        }
                        if (!Arrays.equals(key, check)) {
                          throw new RuntimeException("Key Mismatch");
                        }
                      }
                    }
                    catch (IOException e) {
                      throw Throwables.propagate(e);
                    }
                  }
                }
            );
        try (SocketChannel sendSocket = SocketChannel.open(new InetSocketAddress("localhost", currPort))) {
          sendSocket.write(ByteBuffer.wrap(key));
          future.get(10, TimeUnit.MILLISECONDS);
        }
        catch (TimeoutException ex) {
          throw new IOException("Send took too long", ex);
        }
        catch (ExecutionException ex) {
          throw new IOException("Error in thread", ex);
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw Throwables.propagate(e);
        }
        return currPort;
      }
      catch (IOException e) {
        ++currPort;
      }
      finally {
        executorService.shutdownNow();
      }
    }

    throw new ISE("Unable to find open port between[%d] and [%d]", startPort, currPort);
  }
}
