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

package org.apache.druid.query.operator.window;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.junit.Assert;
import org.junit.Test;

public class ComposingProcessorTest
{
  @Test
  public void testSanity()
  {
    final ProcessorForTesting firstProcessor = new ProcessorForTesting();
    final ProcessorForTesting secondProcessor = new ProcessorForTesting();

    ComposingProcessor proc = new ComposingProcessor(firstProcessor, secondProcessor);

    proc.process(null);
    Assert.assertEquals(1, firstProcessor.processCounter);
    Assert.assertEquals(1, secondProcessor.processCounter);

    proc.process(null);
    Assert.assertEquals(2, firstProcessor.processCounter);
    Assert.assertEquals(2, secondProcessor.processCounter);

    Assert.assertTrue(proc.validateEquivalent(proc));
    Assert.assertEquals(1, firstProcessor.validateCounter);
    Assert.assertEquals(1, secondProcessor.validateCounter);

    firstProcessor.validationResult = false;
    Assert.assertFalse(proc.validateEquivalent(proc));
    Assert.assertEquals(2, firstProcessor.validateCounter);
    Assert.assertEquals(1, secondProcessor.validateCounter);
  }

  private static class ProcessorForTesting implements Processor
  {
    private int processCounter = 0;
    private int validateCounter = 0;
    private boolean validationResult = true;

    @Override
    public RowsAndColumns process(RowsAndColumns incomingPartition)
    {
      ++processCounter;
      return incomingPartition;
    }

    @Override
    public boolean validateEquivalent(Processor otherProcessor)
    {
      ++validateCounter;
      return validationResult;
    }
  }
}
