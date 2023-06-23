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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class InputFormatTest
{
  private static final InputFormat INPUT_FORMAT = new InputFormat()
  {
    @Override
    public boolean isSplittable()
    {
      return false;
    }

    @Override
    public InputEntityReader createReader(InputRowSchema inputRowSchema, InputEntity source, File temporaryDirectory)
    {
      return null;
    }
  };

  @Test
  public void test_getWeightedSize_withoutCompression()
  {
    final long unweightedSize = 100L;
    Assert.assertEquals(unweightedSize, INPUT_FORMAT.getWeightedSize("file.json", unweightedSize));
  }
}
