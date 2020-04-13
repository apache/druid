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

package org.apache.druid.indexing.common.task.batch.parallel.iterator;

import org.apache.druid.data.input.HandlingInputRowIterator;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

@RunWith(Enclosed.class)
public class DefaultIndexTaskInputRowIteratorBuilderTest
{
  public static class BuildTest
  {
    private static final CloseableIterator<InputRow> ITERATOR = EasyMock.mock(CloseableIterator.class);
    private static final GranularitySpec GRANULARITY_SPEC = EasyMock.mock(GranularitySpec.class);
    private static final Runnable NULL_ROW_RUNNABLE = IndexTaskInputRowIteratorBuilder.NOOP_RUNNABLE;
    private static final Consumer<InputRow> ABSENT_BUCKET_INTERVAL_CONSUMER =
        IndexTaskInputRowIteratorBuilder.NOOP_CONSUMER;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void requiresDelegate()
    {
      exception.expect(NullPointerException.class);
      exception.expectMessage("delegate required");

      new DefaultIndexTaskInputRowIteratorBuilder()
          .granularitySpec(GRANULARITY_SPEC)
          .nullRowRunnable(NULL_ROW_RUNNABLE)
          .absentBucketIntervalConsumer(ABSENT_BUCKET_INTERVAL_CONSUMER)
          .build();
    }

    @Test
    public void requiresGranularitySpec()
    {
      exception.expect(NullPointerException.class);
      exception.expectMessage("granularitySpec required");

      new DefaultIndexTaskInputRowIteratorBuilder()
          .delegate(ITERATOR)
          .nullRowRunnable(NULL_ROW_RUNNABLE)
          .absentBucketIntervalConsumer(ABSENT_BUCKET_INTERVAL_CONSUMER)
          .build();
    }

    @Test
    public void requiresNullRowHandler()
    {
      exception.expect(NullPointerException.class);
      exception.expectMessage("nullRowRunnable required");

      new DefaultIndexTaskInputRowIteratorBuilder()
          .delegate(ITERATOR)
          .granularitySpec(GRANULARITY_SPEC)
          .absentBucketIntervalConsumer(ABSENT_BUCKET_INTERVAL_CONSUMER)
          .build();
    }

    @Test
    public void requiresAbsentBucketIntervalHandler()
    {
      exception.expect(NullPointerException.class);
      exception.expectMessage("absentBucketIntervalConsumer required");

      new DefaultIndexTaskInputRowIteratorBuilder()
          .delegate(ITERATOR)
          .granularitySpec(GRANULARITY_SPEC)
          .nullRowRunnable(NULL_ROW_RUNNABLE)
          .build();
    }

    @Test
    public void succeedsIfAllRequiredPresent()
    {
      new DefaultIndexTaskInputRowIteratorBuilder()
          .delegate(ITERATOR)
          .granularitySpec(GRANULARITY_SPEC)
          .nullRowRunnable(NULL_ROW_RUNNABLE)
          .absentBucketIntervalConsumer(ABSENT_BUCKET_INTERVAL_CONSUMER)
          .build();
    }
  }

  public static class HandlerTest
  {
    private static final IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester HANDLER_TESTER =
        IndexTaskInputRowIteratorBuilderTestingFactory.createHandlerTester(
            DefaultIndexTaskInputRowIteratorBuilder::new
        );
    private static final InputRow NO_NEXT_INPUT_ROW = null;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void invokesNullRowHandlerFirst()
    {
      DateTime invalidTimestamp = DateTimes.utc(Long.MAX_VALUE);
      CloseableIterator<InputRow> nullInputRowIterator =
          IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(null);
      GranularitySpec absentBucketIntervalGranularitySpec =
          IndexTaskInputRowIteratorBuilderTestingFactory.createAbsentBucketIntervalGranularitySpec(invalidTimestamp);

      List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
          HANDLER_TESTER.invokeHandlers(
              nullInputRowIterator,
              absentBucketIntervalGranularitySpec,
              NO_NEXT_INPUT_ROW
          );

      Assert.assertEquals(
          Collections.singletonList(IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.NULL_ROW),
          handlerInvocationHistory
      );
    }

    @Test
    public void invokesInvalidTimestampHandlerBeforeAbsentBucketIntervalHandler()
    {
      DateTime invalidTimestamp = DateTimes.utc(Long.MAX_VALUE);
      InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(invalidTimestamp);
      CloseableIterator<InputRow> inputRowIterator =
          IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(inputRow);
      GranularitySpec absentBucketIntervalGranularitySpec =
          IndexTaskInputRowIteratorBuilderTestingFactory.createAbsentBucketIntervalGranularitySpec(invalidTimestamp);

      exception.expect(ParseException.class);
      exception.expectMessage("Encountered row with timestamp that cannot be represented as a long");

      HANDLER_TESTER.invokeHandlers(inputRowIterator, absentBucketIntervalGranularitySpec, NO_NEXT_INPUT_ROW);
    }

    @Test
    public void invokesAbsentBucketIntervalHandlerLast()
    {
      DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
      InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp);
      CloseableIterator<InputRow> inputRowIterator =
          IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(inputRow);
      GranularitySpec absentBucketIntervalGranularitySpec =
          IndexTaskInputRowIteratorBuilderTestingFactory.createAbsentBucketIntervalGranularitySpec(timestamp);

      List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
          HANDLER_TESTER.invokeHandlers(
              inputRowIterator,
              absentBucketIntervalGranularitySpec,
              NO_NEXT_INPUT_ROW
          );

      Assert.assertEquals(
          Collections.singletonList(
              IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.ABSENT_BUCKET_INTERVAL
          ),
          handlerInvocationHistory
      );
    }

    @Test
    public void invokesAppendedHandlersLast()
    {
      DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
      InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp);
      CloseableIterator<InputRow> inputRowIterator =
          IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(inputRow);
      GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
          timestamp,
          IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
      );

      List<HandlingInputRowIterator.InputRowHandler> appendedHandlers = Collections.singletonList(row -> true);

      List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
          HANDLER_TESTER.invokeHandlers(
              inputRowIterator,
              granularitySpec,
              appendedHandlers,
              NO_NEXT_INPUT_ROW
          );

      Assert.assertEquals(
          Collections.singletonList(IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.APPENDED),
          handlerInvocationHistory
      );
    }

    @Test
    public void doesNotInvokeHandlersIfRowValid()
    {
      DateTime timestamp = DateTimes.utc(0);
      InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp);
      CloseableIterator<InputRow> inputRowIterator =
          IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(inputRow);
      GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
          timestamp,
          IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
      );

      List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
          HANDLER_TESTER.invokeHandlers(inputRowIterator, granularitySpec, inputRow);

      Assert.assertEquals(Collections.emptyList(), handlerInvocationHistory);
    }
  }
}
