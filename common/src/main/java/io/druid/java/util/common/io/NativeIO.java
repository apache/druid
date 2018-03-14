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

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;

import com.google.common.annotations.VisibleForTesting;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import io.druid.java.util.common.logger.Logger;

/**
 * Native I/O operations in order to minimize cache impact.
 */
public class NativeIO
{
  private static final Logger log = new Logger(NativeIO.class);

  private static final int POSIX_FADV_DONTNEED = 4; /* fadvise.h */
  private static Field field;

  private static volatile boolean initialized = false;
  private static volatile boolean fadvisePossible = true;

  static {
    field = getFieldByReflection(FileDescriptor.class, "fd");

    try {
      Native.register(Platform.C_LIBRARY_NAME);
      initialized = true;
    }
    catch (NoClassDefFoundError e) {
      log.info("JNA not found. Native methods will be disabled.");
    }
    catch (UnsatisfiedLinkError e) {
      log.info("Unable to link C library. Native methods will be disabled.");
    }
    catch (NoSuchMethodError e) {
      log.warn("Obsolete version of JNA present; unable to register C library");
    }
  }

  private static native int posix_fadvise(int fd, long offset, long len, int flag) throws LastErrorException;

  private NativeIO() {}

  private static Field getFieldByReflection(Class cls, String fieldName)
  {
    Field field = null;

    try {
      field = cls.getDeclaredField(fieldName);
      field.setAccessible(true);
    }
    catch (Exception e) {
      log.warn("Unable to read [%s] field from [%s]", fieldName, cls.getName());
    }

    return field;
  }
  /**
   * Get system file descriptor (int) from FileDescriptor object.
   * @param descriptor - FileDescriptor object to get fd from
   * @return file descriptor, -1 or error
   */
  public static int getfd(FileDescriptor descriptor)
  {
    try {
      return field.getInt(descriptor);
    }
    catch (IllegalArgumentException | IllegalAccessException e) {
      log.warn("Unable to read fd field from java.io.FileDescriptor");
    }

    return -1;
  }

  /**
   * Remove pages from the file system page cache when they wont
   * be accessed again
   *
   * @param fd     The file descriptor of the source file.
   * @param offset The offset within the file.
   * @param len    The length to be flushed.
   */

  public static void trySkipCache(int fd, long offset, long len)
  {
    if (!initialized || !fadvisePossible || fd < 0) {
      return;
    }
    try {
      // we ignore the return value as this is just best effort to avoid the cache
      posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);
    }
    catch (UnsupportedOperationException uoe) {
      log.warn(uoe, "posix_fadvise is not supported");
      fadvisePossible = false;
    }
    catch (UnsatisfiedLinkError ule) {
      // if JNA is unavailable just skipping Direct I/O
      // instance of this class will act like normal RandomAccessFile
      log.warn(ule, "Unsatisfied Link error: posix_fadvise failed on file descriptor [%d], offset [%d]",
          fd, offset);
      fadvisePossible = false;
    }
    catch (Exception e) {
      // This is best effort anyway so lets just log that there was an
      // exception and forget
      log.warn(e, "Unknown exception: posix_fadvise failed on file descriptor [%d], offset [%d]",
          fd, offset);
    }
  }


  /**
   * Copy from an input stream to a file minimizing cache impact on the destination.. This happens chunk by chunk
   * so only at most chunk size will be present in the OS page cache. Posix (Linux, BSD) only.
   *
   * @param src Source InputStream where to copy from
   * @param dest Destination file to copy to
   * @throws IOException
   */
  public static void chunkedCopy(InputStream src, File dest) throws IOException
  {

    final byte[] buf = new byte[8 << 20]; // 8Mb buffer
    long offset = 0;

    try (
      final RandomAccessFile raf = new RandomAccessFile(dest, "rwd")
    ) {
      final int fd = getfd(raf.getFD());

      for (int numBytes = 0, bytesRead = 0; bytesRead > -1; ) {
        bytesRead = src.read(buf, numBytes, buf.length - numBytes);

        if (numBytes >= buf.length || bytesRead == -1) {
          raf.write(buf, 0, numBytes);
          trySkipCache(fd, offset, numBytes);
          offset = raf.getFilePointer();
          numBytes = 0;
        }

        numBytes += bytesRead;
      }
    }
  }

  @VisibleForTesting
  static void setFadvisePossible(boolean setting)
  {
    fadvisePossible = setting;
  }

  @VisibleForTesting
  static boolean getFadvisePossible()
  {
    return fadvisePossible;
  }
}
