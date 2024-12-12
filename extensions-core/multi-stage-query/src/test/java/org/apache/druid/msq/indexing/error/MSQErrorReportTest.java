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

package org.apache.druid.msq.indexing.error;

import org.apache.druid.frame.processor.FrameRowTooLargeException;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.indexing.common.task.batch.TooManyBucketsException;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.groupby.epinephelinae.UnexpectedMultiValueDimensionException;
import org.junit.Assert;
import org.junit.Test;

public class MSQErrorReportTest
{

  public static final String ERROR_MESSAGE = "test";

  @Test
  public void testErrorReportFault()
  {
    Assert.assertEquals(UnknownFault.forException(null), MSQErrorReport.getFaultFromException(null));

    MSQException msqException = new MSQException(null, UnknownFault.forMessage(ERROR_MESSAGE));
    Assert.assertEquals(msqException.getFault(), MSQErrorReport.getFaultFromException(msqException));

    ParseException parseException = new ParseException(null, ERROR_MESSAGE);
    Assert.assertEquals(
        new CannotParseExternalDataFault(ERROR_MESSAGE),
        MSQErrorReport.getFaultFromException(parseException)
    );

    UnsupportedColumnTypeException columnTypeException = new UnsupportedColumnTypeException(ERROR_MESSAGE, null);
    Assert.assertEquals(
        new ColumnTypeNotSupportedFault(ERROR_MESSAGE, null),
        MSQErrorReport.getFaultFromException(columnTypeException)
    );

    TooManyBucketsException tooManyBucketsException = new TooManyBucketsException(10);
    Assert.assertEquals(new TooManyBucketsFault(10), MSQErrorReport.getFaultFromException(tooManyBucketsException));

    FrameRowTooLargeException tooLargeException = new FrameRowTooLargeException(10);
    Assert.assertEquals(new RowTooLargeFault(10), MSQErrorReport.getFaultFromException(tooLargeException));

    UnexpectedMultiValueDimensionException mvException = new UnexpectedMultiValueDimensionException(ERROR_MESSAGE);
    Assert.assertEquals(QueryRuntimeFault.CODE, MSQErrorReport.getFaultFromException(mvException).getErrorCode());

    QueryTimeoutException queryException = new QueryTimeoutException(ERROR_MESSAGE);
    Assert.assertEquals(
        new QueryRuntimeFault(ERROR_MESSAGE, null),
        MSQErrorReport.getFaultFromException(queryException)
    );

    RuntimeException runtimeException = new RuntimeException(ERROR_MESSAGE);
    Assert.assertEquals(
        UnknownFault.forException(runtimeException),
        MSQErrorReport.getFaultFromException(runtimeException)
    );
  }
}
