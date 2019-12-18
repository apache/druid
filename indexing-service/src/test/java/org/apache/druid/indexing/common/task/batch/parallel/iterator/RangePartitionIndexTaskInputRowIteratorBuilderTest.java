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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.segment.indexing.granularity.GranularitySpec;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RangePartitionIndexTaskInputRowIteratorBuilderTest
{
  private static final boolean SKIP_NULL = true;
  private static final IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester HANDLER_TESTER =
      IndexTaskInputRowIteratorBuilderTestingFactory.createHandlerTester(
          () -> new RangePartitionIndexTaskInputRowIteratorBuilder(
              IndexTaskInputRowIteratorBuilderTestingFactory.DIMENSION,
              SKIP_NULL
          )
      );
  private static final InputRow NO_NEXT_INPUT_ROW = null;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @Test
  public void invokesDimensionValueCountFilterLast()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> nullDimensionValue = Collections.emptyList();  // Rows.objectToStrings() returns empty list for null
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(
        timestamp,
        nullDimensionValue
    );
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(
        inputRow
    );
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
        timestamp,
        IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
    );

    List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
        HANDLER_TESTER.invokeHandlers(
            inputRowIterator,
            granularitySpec,
            NO_NEXT_INPUT_ROW
        );

    assertNotInHandlerInvocationHistory(
        handlerInvocationHistory,
        IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.NULL_ROW
    );
    assertNotInHandlerInvocationHistory(
        handlerInvocationHistory,
        IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.ABSENT_BUCKET_INTERVAL
    );
  }

  @Test
  public void throwsExceptionIfMultipleDimensionValues()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> multipleDimensionValues = Arrays.asList("multiple", "dimension", "values");
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(
        timestamp,
        multipleDimensionValues
    );
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(
        inputRow
    );
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
        timestamp,
        IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
    );

    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Cannot partition on multi-value dimension [dimension]");

    HANDLER_TESTER.invokeHandlers(
        inputRowIterator,
        granularitySpec,
        NO_NEXT_INPUT_ROW
    );
  }

  @Test
  public void doesNotInvokeHandlersIfRowValid()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> nullDimensionValue = Collections.singletonList(null);
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp, nullDimensionValue);
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(
        inputRow
    );
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
        timestamp,
        IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
    );

    List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
        HANDLER_TESTER.invokeHandlers(
            inputRowIterator,
            granularitySpec,
            inputRow
        );

    Assert.assertEquals(Collections.emptyList(), handlerInvocationHistory);
  }

  @Test
  public void invokesHandlerIfRowInvalidNull()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> nullDimensionValue = null;
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp, nullDimensionValue);
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(
        inputRow
    );
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
        timestamp,
        IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
    );

    List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
        HANDLER_TESTER.invokeHandlers(
            inputRowIterator,
            granularitySpec,
            NO_NEXT_INPUT_ROW
        );

    assertNotInHandlerInvocationHistory(
        handlerInvocationHistory,
        IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.NULL_ROW
    );
    assertNotInHandlerInvocationHistory(
        handlerInvocationHistory,
        IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler.ABSENT_BUCKET_INTERVAL
    );
  }

  @Test
  public void doesNotInvokeHandlersIfRowValidNull()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> nullDimensionValue = null;
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp, nullDimensionValue);
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(
        inputRow
    );
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(
        timestamp,
        IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT
    );

    IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester handlerTester =
        IndexTaskInputRowIteratorBuilderTestingFactory.createHandlerTester(
            () -> new RangePartitionIndexTaskInputRowIteratorBuilder(
                IndexTaskInputRowIteratorBuilderTestingFactory.DIMENSION,
                !SKIP_NULL
            )
        );
    List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory =
        handlerTester.invokeHandlers(
            inputRowIterator,
            granularitySpec,
            inputRow
        );

    Assert.assertEquals(Collections.emptyList(), handlerInvocationHistory);
  }

  private static void assertNotInHandlerInvocationHistory(
      List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory,
      IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler handler
  )
  {
    Assert.assertThat(handlerInvocationHistory, Matchers.not(Matchers.contains(handler)));
  }
}
