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

package io.druid.java.util.common.io;

import java.io.IOException;
import java.io.RandomAccessFile;


public class NativeIOTest
{
  public static void main(String[] args)
  {
    try {
      RandomAccessFile raf = new RandomAccessFile(args[1], "rwd");
      RandomAccessFile rin = new RandomAccessFile(args[0], "rwd");

      int fd_in = NativeIO.getfd(rin.getFD());
      int fd_out = NativeIO.getfd(raf.getFD());

      System.out.println("FD in: " + fd_in + " FD out: " + fd_out);
      byte[] b = new byte[8 * 1024 * 1024]; // 8 Mb
      int num_bytes = 0;
      long offset_in = 0;
      long offset_out = 0;
      while ((num_bytes = rin.read(b)) > 0) {
        raf.write(b, 0, num_bytes);
        NativeIO.trySkipCache(fd_in, offset_in, num_bytes);
        NativeIO.trySkipCache(fd_out, offset_out, num_bytes);
        offset_in = rin.getFilePointer();
        offset_out = raf.getFilePointer();
        System.out.println("Bytes read: " + raf.getFilePointer() + " now reading: " + num_bytes);
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
