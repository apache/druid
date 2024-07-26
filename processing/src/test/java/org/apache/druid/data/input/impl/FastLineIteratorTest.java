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

package org.apache.druid.data.input.impl;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.druid.common.config.NullHandling;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.NoSuchElementException;

public class FastLineIteratorTest
{
  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setup()
  {
    NullHandling.initializeForTests();
  }

  @Test
  public void testNullInputThrows()
  {
    expectedException.expect(NullPointerException.class);
    //noinspection ResultOfObjectAllocationIgnored
    new FastLineIterator.Strings(null);
  }

  @Test
  public void testEmptyInput()
  {
    byte[] input = new byte[0];
    FastLineIterator<String> iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertFalse(iterator.hasNext());

    expectedException.expect(NoSuchElementException.class);
    iterator.next();
  }

  @Test
  public void testSoloCr()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    // a single \r
    // it is expected that this emits a complete line with \r since a return on its own is not a line break
    input = "\r".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("\r", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSoloLf()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    // a single \n
    // should emit a single complete 'line' as "", and no trailing line (since EOF)
    input = "\n".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testBackwardsLfCr()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    // should emit two lines:
    // first one is an empty line for before the \n,
    // second is the \r alone
    input = "\n\r".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("", iterator.next());
    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("\r", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testForwardsSoloCrLf()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    // should emit one (empty) line
    input = "\r\n".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testSingleLine()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    // without an end
    input = "abcd".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("abcd", iterator.next());
    Assert.assertFalse(iterator.hasNext());

    // with an end
    input = "abcd\n".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("abcd", iterator.next());
    Assert.assertFalse(iterator.hasNext());

    // with an end
    input = "abcd\r\n".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("abcd", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testMultipleLines()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    input = "abcd\ndefg\nhijk".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("abcd", iterator.next());
    Assert.assertEquals("defg", iterator.next());
    Assert.assertEquals("hijk", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testEmptyMiddleLine()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    input = "abcd\n\nhijk\n".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("abcd", iterator.next());
    Assert.assertEquals("", iterator.next());
    Assert.assertEquals("hijk", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testEmptyLastLine()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    input = "abcd\ndefg\nhijk\n".getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals("abcd", iterator.next());
    Assert.assertEquals("defg", iterator.next());
    Assert.assertEquals("hijk", iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testOverlappingBuffer()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    String line1 = randomString(FastLineIterator.BUFFER_SIZE - 20);
    String line2 = randomString(40);
    String line3 = randomString(20);

    input = (line1 + "\n" + line2 + "\n" + line3 + "\n").getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(line1, iterator.next());
    Assert.assertEquals(line2, iterator.next());
    Assert.assertEquals(line3, iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  @Test
  public void testLineLargerThanBufferSize()
  {
    byte[] input;
    FastLineIterator<String> iterator;

    // random lengths that force multiple buffer trips
    String line1 = randomString(FastLineIterator.BUFFER_SIZE * 3 + 10);
    String line2 = randomString(FastLineIterator.BUFFER_SIZE * 2 + 15);
    String line3 = randomString(FastLineIterator.BUFFER_SIZE + 9);

    input = (line1 + "\r\n" + line2 + "\r\n" + line3 + "\r\n").getBytes(StandardCharsets.UTF_8);
    iterator = new FastLineIterator.Strings(new ByteArrayInputStream(input));

    Assert.assertTrue(iterator.hasNext());
    Assert.assertEquals(line1, iterator.next());
    Assert.assertEquals(line2, iterator.next());
    Assert.assertEquals(line3, iterator.next());
    Assert.assertFalse(iterator.hasNext());
  }

  /**
   * Random string that does not contain \r or \n.
   */
  private static String randomString(final int length)
  {
    return RandomStringUtils.random(length)
                            .replace('\r', '?')
                            .replace('\n', '?');
  }
}
