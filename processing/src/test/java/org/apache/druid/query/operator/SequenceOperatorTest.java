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

package org.apache.druid.query.operator;

import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.operator.window.RowsAndColumnsHelper;
import org.apache.druid.query.rowsandcols.MapOfColumnsRowsAndColumns;
import org.apache.druid.query.rowsandcols.column.IntArrayColumn;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SequenceOperatorTest
{
  @Test
  public void testSanity()
  {
    SequenceOperator op = new SequenceOperator(Sequences.simple(Arrays.asList(
        MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{1})),
        MapOfColumnsRowsAndColumns.of("hi", new IntArrayColumn(new int[]{1}))
    )));

    final RowsAndColumnsHelper helper = new RowsAndColumnsHelper()
        .expectColumn("hi", new int[]{1})
        .allColumnsRegistered();

    new OperatorTestHelper()
        .withPushFn(
            rac -> {
              helper.validate(rac);
              return Operator.Signal.GO;
            }
        )
        .withFinalValidation(testReceiver -> Assert.assertEquals(2, testReceiver.getNumPushed()))
        .runToCompletion(op);
  }
}
