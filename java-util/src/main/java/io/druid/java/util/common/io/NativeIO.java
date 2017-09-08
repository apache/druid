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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;


public class NativeIO
{
  private static final Logger LOG = LoggerFactory.getLogger(NativeIO.class);

  private static final int POSIX_FADV_DONTNEED = 4; /* fadvise.h */

  private static boolean initialized = false;
  private static boolean fadvisePossible = true;

    /* private static int PAGE_SIZE = -1; */

  static {
    try {
      Native.register(Platform.C_LIBRARY_NAME);

            /*
            Field unsafeField = Unsafe.class.getField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);

            PAGE_SIZE = unsafe.pageSize();
            */

      initialized = true;
    }
    catch (NoClassDefFoundError e) {
      LOG.info("JNA not found. Native methods will be disabled.");
    }
    catch (UnsatisfiedLinkError e) {
      LOG.info("Unable to link C library. Native methods will be disabled.");
    }
    catch (NoSuchMethodError e) {
      LOG.warn("Obsolete version of JNA present; unable to register C library");
    }
        /*
        catch (NoSuchFieldException e) {
            LOG.error("Cannot get kernel page size");
        }
        catch (IllegalAccessException e) {
            LOG.error("Cannot get kernel page size");
        }
        */
  }

  public static native int posix_fadvise(int fd, long offset, long len, int flag) throws LastErrorException;
  public static native int mincore(long addr, long length, byte[] vec);

  private NativeIO() {}

  private static Field getFieldByReflection(Class cls, String fieldName)
  {
    Field field = null;

    try {
      field = cls.getDeclaredField(fieldName);
      field.setAccessible(true);
    }
    catch (Exception e) {
      // We don't really expect this so throw an assertion to
      // catch this during development
      assert false;
      LOG.warn("Unable to read {} field from {}", fieldName, cls.getName());
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
    Field field = getFieldByReflection(descriptor.getClass(), "fd");
    try {
      return field.getInt(descriptor);
    }
    catch (Exception e) {
      LOG.warn("Unable to read fd field from java.io.FileDescriptor");
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
   *
   * @throws nothing => Best effort
   */

  public static void trySkipCache(int fd, long offset, long len)
  {
    if (!initialized || !fadvisePossible || fd < 0) {
      return;
    }
    try {
      posix_fadvise(fd, offset, len, POSIX_FADV_DONTNEED);
    }
    catch (UnsupportedOperationException uoe) {
      LOG.warn("posix_fadvise is not supported : ", uoe);
      fadvisePossible = false;
    }
    catch (UnsatisfiedLinkError ule) {
      // if JNA is unavailable just skipping Direct I/O
      // instance of this class will act like normal RandomAccessFile
      LOG.warn("Unsatisfied Link error: posix_fadvise failed on file descriptor {}, offset {} : ",
          new Object[] {fd, offset, ule});
      fadvisePossible = false;
    }
    catch (Exception e) {
      // This is best effort anyway so lets just log that there was an
      // exception and forget
      LOG.warn("Unknown exception: posix_fadvise failed on file descriptor {}, offset {} : ",
          new Object[] {fd, offset, e});
    }
  }

  public static void trySkipCache(String path, long offset, long len)
  {
    trySkipCache(getfd(path), offset, len);
  }

    /*
    private static byte[] getMemCachedVec(String fileName) throws IOException,
            NoSuchFieldException, IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        FileChannel fc = FileChannel.open(Paths.get(fileName));
        MappedByteBuffer mmaped = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());

        long addr = getNativeAddress(mmaped);

        int pages = (int) ((fc.size() + (PAGE_SIZE - 1)) / (PAGE_SIZE));
        byte[] vec = new byte[pages];

        int result = mincore(addr, fc.size(), vec);

        if (result != 0) {
            throw new IOException("Call to mincore failed with error: " + result);
        }

        unmap(mmaped);

        return vec;

    }

    private static long getNativeAddress(ByteBuffer map) throws NoSuchFieldException, IllegalAccessException
    {
        final Field field = Buffer.class.getDeclaredField("address");
        field.setAccessible(true);

        return field.getLong(map);
    }

    private static void unmap(final MappedByteBuffer buffer) throws
            IllegalAccessException, InvocationTargetException, NoSuchMethodException
    {
        if (buffer != null) {
            final Method method = FileChannelImpl.class.getDeclaredMethod("unmap", MappedByteBuffer.class);
            method.setAccessible(true);
            method.invoke(null, buffer);
        }
    }
    */

  /**
   * Get system file descriptor (int) from file path
   *
   * @param path The path to the file
   * @return file descriptor, -1 or error
   */
  public static int getfd(String path)
  {
    RandomAccessFile file = null;
    try {
      file = new RandomAccessFile(path, "r");
      return getfd(file.getFD());
    }
    catch (Throwable t) {
      // ignore
      return -1;
    }
    finally {
      try {
        if (file != null) {
          file.close();
        }
      }
      catch (Throwable t) {
        // ignore
      }
    }
  }

  /**
   * Copy from an input stream to a file minimizing cache impact. This happens chunk by chunk
   * so only at most chunk size will be present in the OS page cache. Linux only.
   *
   * @param src Source InputStream where to copy from
   * @param dest Destination file to copy to
   * @throws IOException
   */
  public static void chunkedCopy(InputStream src, File dest) throws IOException
  {
    final RandomAccessFile raf = new RandomAccessFile(dest, "rwd");
    final int fd = getfd(raf.getFD());

    final byte[] buf = new byte[1024 * 1024 * 8]; // 8Mb buffer
    int numBytes;
    long offset = 0;

    while ((numBytes = src.read(buf)) > 0) {
      raf.write(buf, 0, numBytes);
      trySkipCache(fd, offset, numBytes);
      offset = raf.getFilePointer();
    }

    raf.close();
  }
}
