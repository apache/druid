/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.java.util.common;

import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.utils.JvmUtils;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 */
public class ByteBufferUtils
{
  private static final Logger log = new Logger(ByteBufferUtils.class);

  // the following MethodHandle lookup code is adapted from Apache Kafka
  // https://github.com/apache/kafka/blob/e554dc518eaaa0747899e708160275f95c4e525f/clients/src/main/java/org/apache/kafka/common/utils/MappedByteBuffers.java

  // null if unmap is not supported
  private static final MethodHandle UNMAP;

  // null if unmap is supported
  private static final RuntimeException UNMAP_NOT_SUPPORTED_EXCEPTION;

  static {
    Object unmap = null;
    RuntimeException exception = null;
    try {
      unmap = lookupUnmapMethodHandle();
    }
    catch (RuntimeException e) {
      exception = e;
    }
    if (unmap != null) {
      UNMAP = (MethodHandle) unmap;
      UNMAP_NOT_SUPPORTED_EXCEPTION = null;
    } else {
      UNMAP = null;
      UNMAP_NOT_SUPPORTED_EXCEPTION = exception;
    }
  }

  private static void clean(ByteBuffer buffer)
  {
    if (!buffer.isDirect()) {
      throw new IllegalArgumentException("Unmapping only works with direct buffers");
    }
    if (UNMAP == null) {
      throw new UnsupportedOperationException(UNMAP_NOT_SUPPORTED_EXCEPTION);
    }

    try {
      UNMAP.invokeExact(buffer);
    }
    catch (Throwable throwable) {
      throw new RuntimeException("Unable to unmap the mapped buffer", throwable);
    }
  }

  private static MethodHandle lookupUnmapMethodHandle()
  {
    final MethodHandles.Lookup lookup = MethodHandles.lookup();
    try {
      if (JvmUtils.isIsJava9Compatible()) {
        return unmapJava9(lookup);
      } else {
        return unmapJava7Or8(lookup);
      }
    }
    catch (ReflectiveOperationException | RuntimeException e1) {
      throw new UnsupportedOperationException("Unmapping is not supported on this platform, because internal " +
                                              "Java APIs are not compatible with this Druid version", e1);
    }
  }

  /**
   * NB: while Druid no longer support Java 7, this method would still work with that version as well.
   */
  private static MethodHandle unmapJava7Or8(MethodHandles.Lookup lookup) throws ReflectiveOperationException
  {
    // "Compile" a MethodHandle that is roughly equivalent to the following lambda:
    //
    // (ByteBuffer buffer) -> {
    //   sun.misc.Cleaner cleaner = ((java.nio.DirectByteBuffer) byteBuffer).cleaner();
    //   if (nonNull(cleaner))
    //     cleaner.clean();
    //   else
    //     noop(cleaner); // the noop is needed because MethodHandles#guardWithTest always needs both if and else
    // }
    //
    Class<?> directBufferClass = Class.forName("java.nio.DirectByteBuffer");
    Method m = directBufferClass.getMethod("cleaner");
    m.setAccessible(true);
    MethodHandle directBufferCleanerMethod = lookup.unreflect(m);
    Class<?> cleanerClass = directBufferCleanerMethod.type().returnType();
    MethodHandle cleanMethod = lookup.findVirtual(cleanerClass, "clean", MethodType.methodType(void.class));
    MethodHandle nonNullTest = lookup.findStatic(Objects.class, "nonNull",
                                                 MethodType.methodType(boolean.class, Object.class)
    ).asType(MethodType.methodType(boolean.class, cleanerClass));
    MethodHandle noop = MethodHandles.dropArguments(MethodHandles.constant(
        Void.class,
        null
    ).asType(MethodType.methodType(void.class)), 0, cleanerClass);
    MethodHandle unmapper = MethodHandles.filterReturnValue(
        directBufferCleanerMethod,
        MethodHandles.guardWithTest(nonNullTest, cleanMethod, noop)
    ).asType(MethodType.methodType(void.class, ByteBuffer.class));
    return unmapper;
  }

  private static MethodHandle unmapJava9(MethodHandles.Lookup lookup) throws ReflectiveOperationException
  {
    MethodHandle unmapper = lookup.findVirtual(
        UnsafeUtils.theUnsafeClass(),
        "invokeCleaner",
        MethodType.methodType(void.class, ByteBuffer.class)
    );
    return unmapper.bindTo(UnsafeUtils.theUnsafe());
  }

  /**
   * Same as {@link ByteBuffer#allocateDirect(int)}, but returns a closeable {@link ResourceHolder} that
   * frees the buffer upon close.
   *
   * Direct (off-heap) buffers are an alternative to on-heap buffers that allow memory to be managed
   * outside the purview of the garbage collector. It's most useful when allocating big chunks of memory,
   * like processing buffers.
   *
   * Holders cannot be closed more than once. Attempting to close a holder twice will earn you an
   * {@link IllegalStateException}.
   */
  public static ResourceHolder<ByteBuffer> allocateDirect(final int size)
  {
    class DirectByteBufferHolder implements ResourceHolder<ByteBuffer>
    {
      private final AtomicBoolean closed = new AtomicBoolean(false);
      private volatile ByteBuffer buf = ByteBuffer.allocateDirect(size);

      @Override
      public ByteBuffer get()
      {
        final ByteBuffer theBuf = buf;

        if (theBuf == null) {
          throw new ISE("Closed");
        } else {
          return theBuf;
        }
      }

      @Override
      public void close()
      {
        if (closed.compareAndSet(false, true)) {
          final ByteBuffer theBuf = buf;
          buf = null;
          free(theBuf);
        } else {
          throw new ISE("Already closed");
        }
      }
    }

    return new DirectByteBufferHolder();
  }

  /**
   * Releases memory held by the given direct ByteBuffer
   *
   * @param buffer buffer to free
   */
  public static void free(ByteBuffer buffer)
  {
    if (buffer.isDirect()) {
      clean(buffer);
    }
  }

  /**
   * Un-maps the given memory mapped file
   *
   * @param buffer buffer
   */
  public static void unmap(MappedByteBuffer buffer)
  {
    free(buffer);
  }
}
