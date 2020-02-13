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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SpecificFilesLocalInputSourceTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final SpecificFilesLocalInputSource source = new SpecificFilesLocalInputSource(
        ImmutableList.of(new File("foo").getAbsoluteFile(), new File("bar").getAbsoluteFile())
    );
    final byte[] json = mapper.writeValueAsBytes(source);
    final SpecificFilesLocalInputSource fromJson = (SpecificFilesLocalInputSource) mapper.readValue(
        json,
        InputSource.class
    );
    Assert.assertEquals(source, fromJson);
  }

  @Test
  public void testCreateSplitsRespectingSplitHintSpec()
  {
    final long fileSize = 15;
    final long maxSplitSize = 50;
    final List<File> files = prepareFiles(10, fileSize);
    final SpecificFilesLocalInputSource inputSource = new SpecificFilesLocalInputSource(files);
    final List<InputSplit<List<File>>> splits = inputSource
        .createSplits(new NoopInputFormat(), new MaxSizeSplitHintSpec(maxSplitSize))
        .collect(Collectors.toList());
    Assert.assertEquals(4, splits.size());
    Assert.assertEquals(3, splits.get(0).get().size());
    Assert.assertEquals(3, splits.get(1).get().size());
    Assert.assertEquals(3, splits.get(2).get().size());
    Assert.assertEquals(1, splits.get(3).get().size());
  }

  @Test
  public void testEstimateSplitsRespectingSplitHintSpec()
  {
    final long fileSize = 13;
    final long maxSplitSize = 40;
    final List<File> files = prepareFiles(10, fileSize);
    final SpecificFilesLocalInputSource inputSource = new SpecificFilesLocalInputSource(files);
    Assert.assertEquals(
        4,
        inputSource.estimateNumSplits(new NoopInputFormat(), new MaxSizeSplitHintSpec(maxSplitSize))
    );
  }

  private static List<File> prepareFiles(int numFiles, long fileSize)
  {
    final List<File> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = EasyMock.niceMock(File.class);
      EasyMock.expect(file.length()).andReturn(fileSize).anyTimes();
      EasyMock.replay(file);
      files.add(file);
    }
    return files;
  }
}
