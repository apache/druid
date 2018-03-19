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

  /**
   *  Wait upon writeout of all pages in the range before performing the write.
   */
  private static final int SYNC_FILE_RANGE_WAIT_BEFORE = 1;

  /**
   * Initiate writeout of all those dirty pages in the range which are not presently
   * under writeback.
   */
  private static final int SYNC_FILE_RANGE_WRITE = 2;

  /**
   * Wait upon writeout of all pages in the range after performing the write.
   */
  private static final int SYNC_FILE_RANGE_WAIT_AFTER = 4;

  private static Field field;

  private static volatile boolean initialized = false;
  private static volatile boolean fadvisePossible = true;
  private static volatile boolean syncFileRangePossible = true;

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
  private static native int sync_file_range(int fd, long offset, long len, int flags);

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
   * Sync part of an open file to the file system.
   *
   * @param fd      The file descriptor of the source file.
   * @param offset  The offset within the file.
   * @param nbytes  The number of bytes to be synced.
   * @param flags   Signal how to synchronize
   */
  private static void trySyncFileRange(int fd, long offset, long nbytes, int flags)
  {
    if (!initialized || !syncFileRangePossible || fd < 0) {
      return;
    }
    try {
      int ret_code = sync_file_range(fd, offset, nbytes, flags);
      if (ret_code != 0) {
        log.warn("failed on syncing fd [%d], offset [%d], bytes [%d], ret_code [%d], errno [%d]",
            fd, offset, nbytes, ret_code, Native.getLastError());
        return;
      }
    }
    catch (UnsupportedOperationException uoe) {
      log.warn(uoe, "sync_file_range is not supported");
      syncFileRangePossible = false;
    }
    catch (UnsatisfiedLinkError nle) {
      log.warn(nle, "sync_file_range failed on fd [%d], offset [%d], bytes [%d]", fd, offset, nbytes);
      syncFileRangePossible = false;
    }
    catch (Exception e) {
      log.warn(e, "Unknown exception: sync_file_range failed on fd [%d], offset [%d], bytes [%d]",
          fd, offset, nbytes);
      syncFileRangePossible = false;
    }
  }

  /**
   * Copy from an input stream to a file minimizing cache impact on the destination.. This happens chunk by chunk
   * so only at most chunk size will be present in the OS page cache. Posix (Linux, BSD) only. The implementation
   * in this method is based on a post by Linus Torvalds here:
   * http://lkml.iu.edu/hypermail/linux/kernel/1005.2/01845.html
   *
   * @param src Source InputStream where to copy from
   * @param dest Destination file to copy to
   * @throws IOException
   */
  public static void chunkedCopy(InputStream src, File dest) throws IOException
  {

    final byte[] buf = new byte[8 << 20]; // 8Mb buffer
    long offset = 0;
    long lastOffset = 0;

    try (
      final RandomAccessFile raf = new RandomAccessFile(dest, "rw")
    ) {
      final int fd = getfd(raf.getFD());

      for (int numBytes = 0, bytesRead = 0, lastBytes = 0; bytesRead > -1;) {
        bytesRead = src.read(buf, numBytes, buf.length - numBytes);

        if (numBytes >= buf.length || bytesRead == -1) {
          raf.write(buf, 0, numBytes);

          // This won't block, but will start writeout asynchronously
          trySyncFileRange(fd, offset, numBytes, SYNC_FILE_RANGE_WRITE);
          if (offset > 0) {
            // This does a blocking write-and-wait on any old ranges
            trySyncFileRange(fd, lastOffset, lastBytes,
                SYNC_FILE_RANGE_WAIT_BEFORE | SYNC_FILE_RANGE_WRITE | SYNC_FILE_RANGE_WAIT_AFTER);
            // Remove the old range from the cache
            trySkipCache(fd, lastOffset, lastBytes);
          }
          lastOffset = offset;
          offset = raf.getFilePointer();
          numBytes = 0;
        }
        lastBytes = numBytes;

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
  static boolean isFadvisePossible()
  {
    return fadvisePossible;
  }

  @VisibleForTesting
  static void setSyncFileRangePossible(boolean setting)
  {
    syncFileRangePossible = setting;
  }

  @VisibleForTesting
  static boolean isSyncFileRangePossible()
  {
    return syncFileRangePossible;
  }

}
