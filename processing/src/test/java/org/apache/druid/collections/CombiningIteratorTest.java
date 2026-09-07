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

package org.apache.druid.collections;

import com.google.common.collect.PeekingIterator;
import org.easymock.EasyMock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.function.BinaryOperator;

@SuppressWarnings("DoNotMock")
public class CombiningIteratorTest
{
  private CombiningIterator<String> testingIterator;
  private Comparator<String> comparator;
  private BinaryOperator<String> combiningFunction;
  private PeekingIterator<String> peekIterator;

  @BeforeEach
  public void setUp()
  {
    peekIterator = EasyMock.createMock(PeekingIterator.class);
    comparator = EasyMock.createMock(Comparator.class);
    combiningFunction = EasyMock.createMock(BinaryOperator.class);
    testingIterator = CombiningIterator.create(peekIterator, comparator, combiningFunction);
  }

  @AfterEach
  public void tearDown()
  {
    testingIterator = null;
  }

  @Test
  public void testHasNext()
  {
    boolean expected = true;
    EasyMock.expect(peekIterator.hasNext()).andReturn(expected);
    EasyMock.replay(peekIterator);
    boolean actual = testingIterator.hasNext();
    EasyMock.verify(peekIterator);
    Assertions.assertEquals(expected, actual, "The hasNext function is broken");
  }

  @Test
  public void testFalseBranchNext()
  {
    boolean expected = true;
    EasyMock.expect(peekIterator.hasNext()).andReturn(expected);
    expected = false;
    EasyMock.expect(peekIterator.hasNext()).andReturn(expected);
    EasyMock.replay(peekIterator);
    Object res = testingIterator.next();
    EasyMock.verify(peekIterator);
    Assertions.assertNull(res, "Should be null");
  }

  @Test
  public void testNext()
  {
    boolean expected = true;
    EasyMock.expect(peekIterator.hasNext()).andReturn(expected).times(4);
    String defaultString = "S1";
    String resString = "S2";
    EasyMock.expect(peekIterator.next()).andReturn(defaultString);
    EasyMock.expect(combiningFunction.apply(EasyMock.eq(defaultString), EasyMock.isNull()))
        .andReturn(resString);
    EasyMock.expect(peekIterator.next()).andReturn(defaultString);
    EasyMock.expect(comparator.compare(EasyMock.eq(resString), EasyMock.eq(defaultString)))
        .andReturn(0);
    EasyMock.expect(peekIterator.next()).andReturn(defaultString);
    EasyMock.expect(combiningFunction.apply(EasyMock.eq(resString), EasyMock.eq(defaultString)))
        .andReturn(resString);
    EasyMock.expect(comparator.compare(EasyMock.eq(resString), EasyMock.eq(defaultString)))
        .andReturn(1);

    EasyMock.replay(peekIterator);
    EasyMock.replay(combiningFunction);
    EasyMock.replay(comparator);

    String actual = testingIterator.next();
    Assertions.assertEquals(resString, actual);

    EasyMock.verify(peekIterator);
    EasyMock.verify(comparator);
    EasyMock.verify(combiningFunction);
  }

  @Test
  public void testExceptionInNext()
  {
    boolean expected = false;
    EasyMock.expect(peekIterator.hasNext()).andReturn(expected);
    EasyMock.replay(peekIterator);
    Assertions.assertThrows(NoSuchElementException.class, () -> testingIterator.next());
    EasyMock.verify(peekIterator);
  }

  @Test
  public void testRemove()
  {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> testingIterator.remove());
  }
}
