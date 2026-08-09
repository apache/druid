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

package org.apache.druid.testing;

import org.junit.jupiter.api.function.Executable;

public class JupiterAssertions
{
  private JupiterAssertions()
  {
  }

  public static void assertTrue(final String message, final boolean condition)
  {
    org.junit.jupiter.api.Assertions.assertTrue(condition, message);
  }

  public static void assertTrue(final boolean condition)
  {
    org.junit.jupiter.api.Assertions.assertTrue(condition);
  }

  public static void assertFalse(final String message, final boolean condition)
  {
    org.junit.jupiter.api.Assertions.assertFalse(condition, message);
  }

  public static void assertFalse(final boolean condition)
  {
    org.junit.jupiter.api.Assertions.assertFalse(condition);
  }

  public static void fail(final String message)
  {
    org.junit.jupiter.api.Assertions.fail(message);
  }

  public static void fail()
  {
    org.junit.jupiter.api.Assertions.fail();
  }

  public static void assertEquals(final String message, final Object expected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, message);
  }

  public static void assertEquals(final Object expected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  public static void assertNotEquals(final String message, final Object unexpected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual, message);
  }

  public static void assertNotEquals(final Object unexpected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual);
  }

  public static void assertNotEquals(final String message, final long unexpected, final long actual)
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual, message);
  }

  public static void assertNotEquals(final long unexpected, final long actual)
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual);
  }

  public static void assertNotEquals(
      final String message,
      final double unexpected,
      final double actual,
      final double delta
  )
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual, delta, message);
  }

  public static void assertNotEquals(final double unexpected, final double actual, final double delta)
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual, delta);
  }

  public static void assertNotEquals(
      final String message,
      final float unexpected,
      final float actual,
      final float delta
  )
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual, delta, message);
  }

  public static void assertNotEquals(final float unexpected, final float actual, final float delta)
  {
    org.junit.jupiter.api.Assertions.assertNotEquals(unexpected, actual, delta);
  }

  public static void assertArrayEquals(final String message, final Object[] expected, final Object[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final Object[] expected, final Object[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(final String message, final boolean[] expected, final boolean[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final boolean[] expected, final boolean[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(final String message, final byte[] expected, final byte[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final byte[] expected, final byte[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(final String message, final char[] expected, final char[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final char[] expected, final char[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(final String message, final short[] expected, final short[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final short[] expected, final short[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(final String message, final int[] expected, final int[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final int[] expected, final int[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(final String message, final long[] expected, final long[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, message);
  }

  public static void assertArrayEquals(final long[] expected, final long[] actual)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual);
  }

  public static void assertArrayEquals(
      final String message,
      final double[] expected,
      final double[] actual,
      final double delta
  )
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, delta, message);
  }

  public static void assertArrayEquals(final double[] expected, final double[] actual, final double delta)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, delta);
  }

  public static void assertArrayEquals(
      final String message,
      final float[] expected,
      final float[] actual,
      final float delta
  )
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, delta, message);
  }

  public static void assertArrayEquals(final float[] expected, final float[] actual, final float delta)
  {
    org.junit.jupiter.api.Assertions.assertArrayEquals(expected, actual, delta);
  }

  public static void assertEquals(final String message, final double expected, final double actual, final double delta)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, delta, message);
  }

  public static void assertEquals(final String message, final float expected, final float actual, final float delta)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, delta, message);
  }

  public static void assertEquals(final long expected, final long actual)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  public static void assertEquals(final String message, final long expected, final long actual)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, message);
  }

  public static void assertEquals(final double expected, final double actual)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
  }

  public static void assertEquals(final String message, final double expected, final double actual)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, message);
  }

  public static void assertEquals(final double expected, final double actual, final double delta)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, delta);
  }

  public static void assertEquals(final float expected, final float actual, final float delta)
  {
    org.junit.jupiter.api.Assertions.assertEquals(expected, actual, delta);
  }

  public static void assertNotNull(final String message, final Object object)
  {
    org.junit.jupiter.api.Assertions.assertNotNull(object, message);
  }

  public static void assertNotNull(final Object object)
  {
    org.junit.jupiter.api.Assertions.assertNotNull(object);
  }

  public static void assertNull(final String message, final Object object)
  {
    org.junit.jupiter.api.Assertions.assertNull(object, message);
  }

  public static void assertNull(final Object object)
  {
    org.junit.jupiter.api.Assertions.assertNull(object);
  }

  public static void assertSame(final String message, final Object expected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertSame(expected, actual, message);
  }

  public static void assertSame(final Object expected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertSame(expected, actual);
  }

  public static void assertNotSame(final String message, final Object unexpected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertNotSame(unexpected, actual, message);
  }

  public static void assertNotSame(final Object unexpected, final Object actual)
  {
    org.junit.jupiter.api.Assertions.assertNotSame(unexpected, actual);
  }

  public static <T extends Throwable> T assertThrows(final Class<T> type, final Executable executable)
  {
    return org.junit.jupiter.api.Assertions.assertThrows(type, executable);
  }

  public static <T extends Throwable> T assertThrows(
      final String message,
      final Class<T> type,
      final Executable executable
  )
  {
    return org.junit.jupiter.api.Assertions.assertThrows(type, executable, message);
  }
}
