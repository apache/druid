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

package org.apache.druid.segment.data;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConstantUtf8IndexedTest
{
  @Test
  void testGetReturnsAFreshBufferEachCallSoDecodingDoesNotConsumeSharedState()
  {
    final ConstantUtf8Indexed indexed =
        new ConstantUtf8Indexed(StringUtils.toUtf8ByteBuffer("acme"));
    // StringUtils.fromUtf8 advances the buffer's position; a shared buffer would be drained after the first read.
    Assertions.assertEquals("acme", StringUtils.fromUtf8(indexed.get(0)));
    Assertions.assertEquals("acme", StringUtils.fromUtf8(indexed.get(0)));
    Assertions.assertEquals(1, indexed.size());
    Assertions.assertTrue(indexed.isSorted());
  }

  @Test
  void testIndexOfFollowsSortedBinarySearchContract()
  {
    // value "m": found -> 0; a value that sorts before -> insertion point 0 -> -1; one that sorts after -> point 1
    // -> -2 (the -(insertionPoint) - 1 contract that GenericIndexed uses, required because isSorted() is true).
    final ConstantUtf8Indexed indexed =
        new ConstantUtf8Indexed(StringUtils.toUtf8ByteBuffer("m"));
    Assertions.assertEquals(0, indexed.indexOf(StringUtils.toUtf8ByteBuffer("m")));
    Assertions.assertEquals(-1, indexed.indexOf(StringUtils.toUtf8ByteBuffer("a")));
    Assertions.assertEquals(-2, indexed.indexOf(StringUtils.toUtf8ByteBuffer("z")));
    // null sorts before any non-null value -> insertion point 0 -> -1.
    Assertions.assertEquals(-1, indexed.indexOf(null));
  }

  @Test
  void testNullValue()
  {
    final ConstantUtf8Indexed indexed = new ConstantUtf8Indexed(null);
    Assertions.assertEquals(1, indexed.size());
    Assertions.assertNull(indexed.get(0));
    Assertions.assertEquals(0, indexed.indexOf(null));
    // a non-null value sorts after the null entry -> insertion point 1 -> -2.
    Assertions.assertEquals(-2, indexed.indexOf(StringUtils.toUtf8ByteBuffer("acme")));
  }

  @Test
  void testGetOutOfBoundsThrows()
  {
    final ConstantUtf8Indexed indexed =
        new ConstantUtf8Indexed(StringUtils.toUtf8ByteBuffer("acme"));
    Assertions.assertThrows(DruidException.class, () -> indexed.get(1));
  }
}
