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

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowListPlusRawValues;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class TimedShutoffInputSourceTest
{
  @Test
  public void testTimeoutShutoff() throws IOException, InterruptedException
  {
    final int timeoutMs = 2000;
    final InputSource inputSource = new TimedShutoffInputSource(
        new InlineInputSource("this,is,test\nthis,data,has\n3,rows,\n"),
        DateTimes.nowUtc().plusMillis(timeoutMs)
    );
    final InputFormat inputFormat = new CsvInputFormat(ImmutableList.of("col1", "col2", "col3"), null, false, 0);
    final InputSourceReader reader = inputSource.reader(
        new InputRowSchema(new TimestampSpec(null, null, null), new DimensionsSpec(null), Collections.emptyList()),
        inputFormat,
        null
    );
    try (CloseableIterator<InputRowListPlusRawValues> iterator = reader.sample()) {
      Thread.sleep(timeoutMs + 1000);
      Assert.assertFalse(iterator.hasNext());
    }
  }
}
