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
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RangePartitionTaskInputRowIteratorBuilderTest
{
  private static final IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester HANDLER_TESTER =
      IndexTaskInputRowIteratorBuilderTestingFactory.createHandlerTester(() -> new RangePartitionIndexTaskInputRowIteratorBuilder(IndexTaskInputRowIteratorBuilderTestingFactory.DIMENSION));
  private static final InputRow NO_NEXT_INPUT_ROW = null;

  @Test
  public void invokesDimensionValueCountFilterLast()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> multipleDimensionValues = Arrays.asList("multiple", "dimension", "values");
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp, multipleDimensionValues);
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(inputRow);
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(timestamp, IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT);

    List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory = HANDLER_TESTER.invokeHandlers(
        inputRowIterator,
        granularitySpec,
        NO_NEXT_INPUT_ROW
    );

    Assert.assertEquals(Collections.emptyList(), handlerInvocationHistory);
  }

  @Test
  public void doesNotInvokeHandlersIfRowValid()
  {
    DateTime timestamp = IndexTaskInputRowIteratorBuilderTestingFactory.TIMESTAMP;
    List<String> singleDimensionValue = Collections.singletonList("single-dimension-value");
    InputRow inputRow = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRow(timestamp, singleDimensionValue);
    CloseableIterator<InputRow> inputRowIterator = IndexTaskInputRowIteratorBuilderTestingFactory.createInputRowIterator(inputRow);
    GranularitySpec granularitySpec = IndexTaskInputRowIteratorBuilderTestingFactory.createGranularitySpec(timestamp, IndexTaskInputRowIteratorBuilderTestingFactory.PRESENT_BUCKET_INTERVAL_OPT);

    List<IndexTaskInputRowIteratorBuilderTestingFactory.HandlerTester.Handler> handlerInvocationHistory = HANDLER_TESTER.invokeHandlers(
        inputRowIterator,
        granularitySpec,
        inputRow
    );

    Assert.assertEquals(Collections.emptyList(), handlerInvocationHistory);
  }
}
