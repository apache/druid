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

package org.apache.druid.data.input;

import org.apache.druid.data.input.HandlingInputRowIterator.InputRowHandler;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HandlingInputRowIteratorTest
{
  public static class AbsentRowTest
  {
    private static final CloseableIterator<InputRow> EMPTY_ITERATOR = CloseableIterators.withEmptyBaggage(
        new Iterator<InputRow>()
        {
          @Override
          public boolean hasNext()
          {
            return false;
          }

          @Nullable
          @Override
          public InputRow next()
          {
            throw new NoSuchElementException();
          }
        });

    private HandlingInputRowIterator target;

    @Before
    public void setup()
    {
      target = new HandlingInputRowIterator(EMPTY_ITERATOR, Collections.emptyList());
    }

    @Test
    public void doesNotHaveNext()
    {
      Assert.assertFalse(target.hasNext());
    }

    @Test(expected = NoSuchElementException.class)
    public void throwsExceptionWhenYieldingNext()
    {
      target.next();
    }
  }

  public static class PresentRowTest
  {

    private static final InputRow INPUT_ROW1 = mock(InputRow.class);
    private static final InputRow INPUT_ROW2 = mock(InputRow.class);
    private static final List<InputRow> INPUT_ROWS = Arrays.asList(INPUT_ROW1, INPUT_ROW2);

    private InputRowHandler successfulHandler;
    private InputRowHandler unsuccessfulHandler;

    @Before
    public void setup()
    {
      // Construct mock object
      successfulHandler = mock(HandlingInputRowIterator.InputRowHandler.class);
      // Method Stubs
      when(successfulHandler.handle(any(InputRow.class))).thenReturn(true);
      // Construct mock object
      unsuccessfulHandler = mock(HandlingInputRowIterator.InputRowHandler.class);
      // Method Stubs
      when(unsuccessfulHandler.handle(any(InputRow.class))).thenReturn(false);
    }

    @Test
    public void hasNext()
    {
      HandlingInputRowIterator target = createInputRowIterator(unsuccessfulHandler, unsuccessfulHandler);
      Assert.assertTrue(target.hasNext());
      Mockito.verify(unsuccessfulHandler, Mockito.never()).handle(INPUT_ROW1);
    }

    @Test
    public void yieldsNextIfUnhandled()
    {
      HandlingInputRowIterator target = createInputRowIterator(unsuccessfulHandler, unsuccessfulHandler);
      Assert.assertEquals(INPUT_ROW1, target.next());
      Mockito.verify(unsuccessfulHandler, Mockito.times(2)).handle(INPUT_ROW1);
    }

    @Test
    public void yieldsNullIfHandledByFirst()
    {
      HandlingInputRowIterator target = createInputRowIterator(successfulHandler, unsuccessfulHandler);
      Assert.assertNull(target.next());
      Mockito.verify(successfulHandler, Mockito.times(1)).handle(INPUT_ROW1);
      Mockito.verify(unsuccessfulHandler, Mockito.never()).handle(INPUT_ROW1);
    }

    @Test
    public void yieldsNullIfHandledBySecond()
    {
      HandlingInputRowIterator target = createInputRowIterator(unsuccessfulHandler, successfulHandler);
      Assert.assertNull(target.next());
      Mockito.verify(unsuccessfulHandler, Mockito.times(1)).handle(INPUT_ROW1);
      Mockito.verify(successfulHandler, Mockito.times(1)).handle(INPUT_ROW1);
    }

    private static HandlingInputRowIterator createInputRowIterator(
        HandlingInputRowIterator.InputRowHandler firstHandler,
        HandlingInputRowIterator.InputRowHandler secondHandler)
    {
      CloseableIterator<InputRow> iterator = CloseableIterators.withEmptyBaggage(
          new Iterator<InputRow>()
          {
            private final Iterator<InputRow> delegate = INPUT_ROWS.iterator();

            @Override
            public boolean hasNext()
            {
              return delegate.hasNext();
            }

            @Nullable
            @Override
            public InputRow next()
            {
              return delegate.next();
            }
          });

      return new HandlingInputRowIterator(iterator, Arrays.asList(firstHandler, secondHandler));
    }
  }
}
