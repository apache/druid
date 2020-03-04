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
import com.google.common.collect.Lists;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.utils.Streams;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class LocalInputSourceTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    final LocalInputSource source = new LocalInputSource(new File("myFile").getAbsoluteFile(), "myFilter");
    final byte[] json = mapper.writeValueAsBytes(source);
    final LocalInputSource fromJson = (LocalInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(source, fromJson);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(LocalInputSource.class).usingGetClass().withNonnullFields("files").verify();
  }

  @Test
  public void testCreateSplitsRespectingSplitHintSpec()
  {
    final long fileSize = 15;
    final long maxSplitSize = 50;
    final Set<File> files = mockFiles(10, fileSize);
    final LocalInputSource inputSource = new LocalInputSource(null, null, files);
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
  public void testEstimateNumSplitsRespectingSplitHintSpec()
  {
    final long fileSize = 13;
    final long maxSplitSize = 40;
    final Set<File> files = mockFiles(10, fileSize);
    final LocalInputSource inputSource = new LocalInputSource(null, null, files);
    Assert.assertEquals(
        4,
        inputSource.estimateNumSplits(new NoopInputFormat(), new MaxSizeSplitHintSpec(maxSplitSize))
    );
  }

  @Test
  public void testGetFileIteratorWithBothBaseDirAndDuplicateFilesIteratingFilesOnlyOnce() throws IOException
  {
    File baseDir = temporaryFolder.newFolder();
    List<File> filesInBaseDir = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      filesInBaseDir.add(file);
    }
    Set<File> files = new HashSet<>(filesInBaseDir.subList(0, 5));
    for (int i = 0; i < 3; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      files.add(file);
    }
    Set<File> expectedFiles = new HashSet<>(filesInBaseDir);
    expectedFiles.addAll(files);
    File.createTempFile("local-input-source", ".filtered", baseDir);
    Iterator<File> fileIterator = new LocalInputSource(baseDir, "*.data", files).getFileIterator();
    Set<File> actualFiles = Streams.sequentialStreamFrom(fileIterator).collect(Collectors.toSet());
    Assert.assertEquals(expectedFiles, actualFiles);
  }

  @Test
  public void testGetFileIteratorWithOnlyBaseDirIteratingAllFiles() throws IOException
  {
    File baseDir = temporaryFolder.newFolder();
    Set<File> filesInBaseDir = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      filesInBaseDir.add(file);
    }
    Iterator<File> fileIterator = new LocalInputSource(baseDir, "*", null).getFileIterator();
    Set<File> actualFiles = Streams.sequentialStreamFrom(fileIterator).collect(Collectors.toSet());
    Assert.assertEquals(filesInBaseDir, actualFiles);
  }

  @Test
  public void testGetFileIteratorWithOnlyFilesIteratingAllFiles() throws IOException
  {
    File baseDir = temporaryFolder.newFolder();
    Set<File> filesInBaseDir = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      final File file = File.createTempFile("local-input-source", ".data", baseDir);
      try (Writer writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8)) {
        writer.write("test");
      }
      filesInBaseDir.add(file);
    }
    Iterator<File> fileIterator = new LocalInputSource(null, null, filesInBaseDir).getFileIterator();
    Set<File> actualFiles = Streams.sequentialStreamFrom(fileIterator).collect(Collectors.toSet());
    Assert.assertEquals(filesInBaseDir, actualFiles);
  }

  @Test
  public void testFileIteratorWithEmptyFilesIteratingNonEmptyFilesOnly()
  {
    final Set<File> files = new HashSet<>(mockFiles(10, 5));
    files.addAll(mockFiles(10, 0));
    final LocalInputSource inputSource = new LocalInputSource(null, null, files);
    List<File> iteratedFiles = Lists.newArrayList(inputSource.getFileIterator());
    Assert.assertTrue(iteratedFiles.stream().allMatch(file -> file.length() > 0));
  }

  private static Set<File> mockFiles(int numFiles, long fileSize)
  {
    final Set<File> files = new HashSet<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = EasyMock.niceMock(File.class);
      EasyMock.expect(file.length()).andReturn(fileSize).anyTimes();
      EasyMock.replay(file);
      files.add(file);
    }
    return files;
  }
}
