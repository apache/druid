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

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.java.util.common.ISE;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LocalInputSourceAdapterTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAdapterGet()
  {
    LocalInputSourceAdapter localInputSourceAdapter = new LocalInputSourceAdapter();
    Assert.assertTrue(localInputSourceAdapter.generateInputSource(Arrays.asList(
        "foo.parquet",
        "bar.parquet"
    )) instanceof LocalInputSource);
  }

  @Test
  public void testAdapterSetup()
  {
    LocalInputSourceAdapter localInputSourceAdapter = new LocalInputSourceAdapter();
    localInputSourceAdapter.setupInputSource(Arrays.asList(
        "foo.parquet",
        "bar.parquet"
    ));
    Assert.assertTrue(localInputSourceAdapter.getInputSource() instanceof LocalInputSource);
  }

  @Test
  public void testEmptyInputSource() throws IOException
  {
    InputFormat mockFormat = EasyMock.createMock(InputFormat.class);
    SplitHintSpec mockSplitHint = EasyMock.createMock(SplitHintSpec.class);
    LocalInputSourceAdapter localInputSourceAdapter = new LocalInputSourceAdapter();
    localInputSourceAdapter.setupInputSource(Collections.emptyList());
    SplittableInputSource<Object> emptyInputSource =
        (SplittableInputSource<Object>) localInputSourceAdapter.getInputSource();
    List<InputSplit<Object>> splitList = emptyInputSource
        .createSplits(mockFormat, mockSplitHint)
        .collect(Collectors.toList());
    Assert.assertTrue(splitList.isEmpty());
    Assert.assertFalse(emptyInputSource.isSplittable());
    Assert.assertFalse(emptyInputSource.needsFormat());
    Assert.assertNull(emptyInputSource.withSplit(EasyMock.createMock(InputSplit.class)));
    Assert.assertEquals(0, emptyInputSource.estimateNumSplits(mockFormat, mockSplitHint));
    Assert.assertFalse(emptyInputSource.reader(
        EasyMock.createMock(InputRowSchema.class),
        mockFormat,
        temporaryFolder.newFolder()
    ).read().hasNext());
  }

  @Test
  public void testIllegalInputSourceGet()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Inputsource is not initialized yet!");
    LocalInputSourceAdapter localInputSourceAdapter = new LocalInputSourceAdapter();
    localInputSourceAdapter.getInputSource();
  }

  @Test
  public void testIllegalInputSourceSetup()
  {
    expectedException.expect(ISE.class);
    expectedException.expectMessage("Inputsource is already initialized!");
    LocalInputSourceAdapter localInputSourceAdapter = new LocalInputSourceAdapter();
    localInputSourceAdapter.setupInputSource(Collections.emptyList());
    localInputSourceAdapter.setupInputSource(Collections.emptyList());
  }
}
