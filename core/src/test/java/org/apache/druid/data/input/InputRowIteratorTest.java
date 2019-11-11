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

import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

@RunWith(Enclosed.class)
public class InputRowIteratorTest
{
  public static class AbsentRowTest
  {
    private static final Firehose EMPTY_FIREHOSE = new TestFirehose()
    {
      @Override
      public boolean hasMore()
      {
        return false;
      }

      @Nullable
      @Override
      public InputRow nextRow()
      {
        return null;
      }
    };

    private InputRowIterator target;

    @Before
    public void setup()
    {
      target = new InputRowIterator(EMPTY_FIREHOSE, Collections.emptyList());
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
    private static final InputRow INPUT_ROW1 = EasyMock.mock(InputRow.class);
    private static final InputRow INPUT_ROW2 = EasyMock.mock(InputRow.class);
    private static final List<InputRow> INPUT_ROWS = Arrays.asList(INPUT_ROW1, INPUT_ROW2);

    private TestInputRowHandler successfulHandler;
    private TestInputRowHandler unsuccessfulHandler;

    @Before
    public void setup()
    {
      successfulHandler = new TestInputRowHandler(true);
      unsuccessfulHandler = new TestInputRowHandler(false);
    }

    @Test
    public void hasNext()
    {
      InputRowIterator target = createInputRowIterator(unsuccessfulHandler, unsuccessfulHandler);
      Assert.assertTrue(target.hasNext());
      Assert.assertFalse(unsuccessfulHandler.invoked);
    }

    @Test
    public void yieldsNextIfUnhandled()
    {
      InputRowIterator target = createInputRowIterator(unsuccessfulHandler, unsuccessfulHandler);
      Assert.assertEquals(INPUT_ROW1, target.next());
      Assert.assertTrue(unsuccessfulHandler.invoked);
    }

    @Test
    public void yieldsNullIfHandledByFirst()
    {
      InputRowIterator target = createInputRowIterator(successfulHandler, unsuccessfulHandler);
      Assert.assertNull(target.next());
      Assert.assertTrue(successfulHandler.invoked);
      Assert.assertFalse(unsuccessfulHandler.invoked);
    }

    @Test
    public void yieldsNullIfHandledBySecond()
    {
      InputRowIterator target = createInputRowIterator(unsuccessfulHandler, successfulHandler);
      Assert.assertNull(target.next());
      Assert.assertTrue(unsuccessfulHandler.invoked);
      Assert.assertTrue(successfulHandler.invoked);
    }

    private static InputRowIterator createInputRowIterator(
        InputRowIterator.InputRowHandler firstHandler,
        InputRowIterator.InputRowHandler secondHandler
    )
    {
      Firehose firehose = new TestFirehose()
      {
        private final Iterator<InputRow> delegate = INPUT_ROWS.iterator();

        @Override
        public boolean hasMore()
        {
          return delegate.hasNext();
        }

        @Nullable
        @Override
        public InputRow nextRow()
        {
          return delegate.next();
        }
      };

      return new InputRowIterator(firehose, Arrays.asList(firstHandler, secondHandler));
    }

    private static class TestInputRowHandler implements InputRowIterator.InputRowHandler
    {
      boolean invoked = false;

      private final boolean successful;

      TestInputRowHandler(boolean successful)
      {
        this.successful = successful;
      }

      @Override
      public boolean handle(InputRow inputRow)
      {
        invoked = true;
        return successful;
      }
    }
  }

  private abstract static class TestFirehose implements Firehose
  {
    @Override
    public void close()
    {
      throw new UnsupportedOperationException();
    }
  }
}
