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
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class LocalInputSourceAdapterTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testAdapterGet()
  {
    LocalInputSourceBuilder localInputSourceAdapter = new LocalInputSourceBuilder();
    Assert.assertTrue(localInputSourceAdapter.generateInputSource(Arrays.asList(
        "foo.parquet",
        "bar.parquet"
    )) instanceof LocalInputSource);
  }

  @Test
  public void testAdapterSetup()
  {
    LocalInputSourceBuilder localInputSourceAdapter = new LocalInputSourceBuilder();
    InputSource delegateInputSource = localInputSourceAdapter.setupInputSource(Arrays.asList(
        "foo.parquet",
        "bar.parquet"
    ));
    Assert.assertTrue(delegateInputSource instanceof LocalInputSource);
  }

  @Test
  public void testEmptyInputSource() throws IOException
  {
    InputFormat mockFormat = EasyMock.createMock(InputFormat.class);
    SplitHintSpec mockSplitHint = EasyMock.createMock(SplitHintSpec.class);
    LocalInputSourceBuilder localInputSourceAdapter = new LocalInputSourceBuilder();
    SplittableInputSource<Object> emptyInputSource =
        (SplittableInputSource<Object>) localInputSourceAdapter.setupInputSource(Collections.emptyList());
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
}
