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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

class SegmentFileBuilderV10Test
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @TempDir
  File tempDir;

  @Test
  void testOneContainerPerProjection() throws IOException
  {
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    // matches the production usage pattern in IndexMergerV10: call startFileGroup then write that projection's
    // columns, then move on to the next projection.
    final String[] projections = {"__base", "projA", "projB"};
    final int colCount = 3;
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (String projection : projections) {
        builder.startFileGroup(projection);
        for (int col = 0; col < colCount; col++) {
          final String name = projection + "/col" + col;
          final File tmpFile = new File(tempDir, StringUtils.format("%s-%s.bin", projection, col));
          Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
          builder.add(name, tmpFile);
        }
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();

      Assertions.assertEquals(projections.length, metadata.getContainers().size());
      Assertions.assertEquals(projections.length * 3, metadata.getFiles().size());
      assertNoContainerMixesProjections(metadata);

      assertColumns(projections, colCount, mapper);
    }
  }

  @Test
  void testProjectionNameWithSlashRoutesCorrectly() throws IOException
  {
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    final String slashyProjection = "nested/projection";
    final int colCount = 3;
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileGroup("__base");
      for (int col = 0; col < colCount; col++) {
        final String name = "__base/col" + col;
        final File tmpFile = new File(tempDir, StringUtils.format("base-%s.bin", col));
        Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
        builder.add(name, tmpFile);
      }
      builder.startFileGroup(slashyProjection);
      for (int col = 0; col < colCount; col++) {
        final String name = slashyProjection + "/col" + col;
        final File tmpFile = new File(tempDir, StringUtils.format("slashy-%s.bin", col));
        Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
        builder.add(name, tmpFile);
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      // 2 projections, 2 containers, even though the slashy name's first '/' would have parsed as projection "nested"
      Assertions.assertEquals(2, metadata.getContainers().size());
      Assertions.assertEquals(2 * colCount, metadata.getFiles().size());

      // round-trip both sets of files
      for (int col = 0; col < colCount; col++) {
        final String baseName = "__base/col" + col;
        final ByteBuffer baseBuf = mapper.mapFile(baseName);
        Assertions.assertNotNull(baseBuf, baseName);
        Assertions.assertEquals(baseName.hashCode(), baseBuf.getInt(), baseName);

        final String slashyName = slashyProjection + "/col" + col;
        final ByteBuffer slashyBuf = mapper.mapFile(slashyName);
        Assertions.assertNotNull(slashyBuf, slashyName);
        Assertions.assertEquals(slashyName.hashCode(), slashyBuf.getInt(), slashyName);
      }
    }
  }

  @Test
  void testStartFileGroupWhileWriterInUseThrows() throws IOException
  {
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileGroup("__base");
      try (SegmentFileChannel outer = builder.addWithChannel("__base/col0", 4)) {
        Assertions.assertThrows(RuntimeException.class, () -> builder.startFileGroup("projA"));
        outer.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
      }
    }
  }

  @Test
  void testExternalBuilderAlsoSplitsContainersByProjection() throws IOException
  {
    final String externalName = "external.segment";
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    final String[] mainProjections = {"__base", "projA", "projB"};
    final String[] externalProjections = {"extProjX", "extProjY"};
    final int colCount = 3;

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (String projection : mainProjections) {
        builder.startFileGroup(projection);
        for (int col = 0; col < colCount; col++) {
          final String name = projection + "/col" + col;
          final File tmpFile = new File(tempDir, StringUtils.format("main-%s-%s.bin", projection, col));
          Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
          builder.add(name, tmpFile);
        }
      }

      // getExternalBuilder returns the SegmentFileBuilder interface but under the hood produces an independent V10
      // sub-file with its own header + containers. Projection-per-container splitting must apply there too.
      final SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      for (String projection : externalProjections) {
        external.startFileGroup(projection);
        for (int col = 0; col < colCount; col++) {
          final String name = projection + "/col" + (col + 1000);
          final File tmpFile = new File(tempDir, StringUtils.format("ext-%s-%s.bin", projection, col));
          Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
          external.add(name, tmpFile);
        }
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    final File externalFile = new File(baseDir, externalName);
    Assertions.assertTrue(segmentFile.exists(), "main v10 file missing");
    Assertions.assertTrue(externalFile.exists(), "external v10 file missing");

    // the external file on its own is a well-formed V10 sub-segment, load it directly to check its container layout.
    try (SegmentFileMapperV10 externalOnly = SegmentFileMapperV10.create(externalFile, JSON_MAPPER)) {
      final SegmentFileMetadata externalMetadata = externalOnly.getSegmentFileMetadata();
      Assertions.assertEquals(externalProjections.length, externalMetadata.getContainers().size());
      Assertions.assertEquals(externalProjections.length * colCount, externalMetadata.getFiles().size());
      assertNoContainerMixesProjections(externalMetadata);
    }

    // loaded together: main file checks its own containers and the external is attached for mapExternalFile().
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER, List.of(externalName))) {
      final SegmentFileMetadata mainMetadata = mapper.getSegmentFileMetadata();
      Assertions.assertEquals(mainProjections.length, mainMetadata.getContainers().size());
      Assertions.assertEquals(mainProjections.length * colCount, mainMetadata.getFiles().size());
      assertNoContainerMixesProjections(mainMetadata);

      assertColumns(mainProjections, colCount, mapper);

      for (String projection : externalProjections) {
        for (int col = 0; col < colCount; col++) {
          final String name = projection + "/col" + (col + 1000);
          final ByteBuffer buf = mapper.mapExternalFile(externalName, name);
          Assertions.assertNotNull(buf, name);
          Assertions.assertEquals(name.hashCode(), buf.getInt(), name);
        }
      }
    }
  }

  @Test
  void testNestedAddWithChannelDelegatesPerBuilder() throws IOException
  {
    // exercises the delegate-temp-file path on both the main and external builders: while an outer addWithChannel is
    // mid-write on a builder, a nested addWithChannel on the same builder must route through a temp file and then be
    // merged back in at outer-close. Main and external each drive this independently, and since they share baseDir,
    // their delegate file names must not collide.
    final String externalName = "external.segment";
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    final byte[] outerBytes = new byte[]{1, 2, 3, 4};
    final byte[] nestedBytes = new byte[]{5, 6, 7, 8};

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileGroup("__base");
      try (SegmentFileChannel outer = builder.addWithChannel("__base/outer", outerBytes.length)) {
        // nested write while outer is in use → forced into delegate temp file
        try (SegmentFileChannel nested = builder.addWithChannel("__base/nested", nestedBytes.length)) {
          nested.write(ByteBuffer.wrap(nestedBytes));
        }
        outer.write(ByteBuffer.wrap(outerBytes));
      }

      final SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      external.startFileGroup("extProj");
      try (SegmentFileChannel extOuter = external.addWithChannel("extProj/outer", outerBytes.length)) {
        try (SegmentFileChannel extNested = external.addWithChannel("extProj/nested", nestedBytes.length)) {
          extNested.write(ByteBuffer.wrap(nestedBytes));
        }
        extOuter.write(ByteBuffer.wrap(outerBytes));
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER, List.of(externalName))) {
      assertBytes(mapper.mapFile("__base/outer"), outerBytes);
      assertBytes(mapper.mapFile("__base/nested"), nestedBytes);
      assertBytes(mapper.mapExternalFile(externalName, "extProj/outer"), outerBytes);
      assertBytes(mapper.mapExternalFile(externalName, "extProj/nested"), nestedBytes);
    }
  }

  @Test
  void testNestedDelegateClosedAfterOuterRoutesToOriginalGroup() throws IOException
  {
    // doing something like this is weird and probably should happen in practice, but if a nested write was requested
    // while file group "groupA" was active; even if the caller switches to "groupB" before finally closing the nested
    // channel, the delegated bytes must still land in groupA's container, not groupB's. Otherwise the grouping breaks,
    // and files from other groups end up in the same container.
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    final byte[] outerBytes = new byte[]{1, 2, 3, 4};
    final byte[] nestedBytes = new byte[]{5, 6, 7, 8};
    final byte[] groupBBytes = new byte[]{9, 10, 11, 12};

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileGroup("groupA");

      final SegmentFileChannel outer = builder.addWithChannel("groupA/outer", outerBytes.length);
      final SegmentFileChannel nested = builder.addWithChannel("groupA/nested", nestedBytes.length);
      nested.write(ByteBuffer.wrap(nestedBytes));

      // close the outer first so writerCurrentlyInUse clears while the nested delegate is still open
      outer.write(ByteBuffer.wrap(outerBytes));
      outer.close();

      // switch group before closing the still-open nested delegate; merge must use the snapshotted "groupA"
      builder.startFileGroup("groupB");
      nested.close();

      // and a real groupB file so we can verify groupB's container is independent of the nested file
      try (SegmentFileChannel groupBFile = builder.addWithChannel("groupB/file", groupBBytes.length)) {
        groupBFile.write(ByteBuffer.wrap(groupBBytes));
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();

      // the nested file was requested under groupA, so it must share groupA's container with groupA/outer
      // and must NOT be in groupB's container alongside groupB/file.
      final int outerContainer = metadata.getFiles().get("groupA/outer").getContainer();
      final int nestedContainer = metadata.getFiles().get("groupA/nested").getContainer();
      final int groupBContainer = metadata.getFiles().get("groupB/file").getContainer();
      Assertions.assertEquals(outerContainer, nestedContainer, "nested delegate landed in the wrong container");
      Assertions.assertNotEquals(groupBContainer, nestedContainer, "nested delegate leaked into groupB's container");

      assertBytes(mapper.mapFile("groupA/outer"), outerBytes);
      assertBytes(mapper.mapFile("groupA/nested"), nestedBytes);
      assertBytes(mapper.mapFile("groupB/file"), groupBBytes);
    }
  }

  @Test
  void testUnprefixedFilesShareSingleContainer() throws IOException
  {
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (int i = 0; i < 5; ++i) {
        final File tmpFile = new File(tempDir, StringUtils.format("plain-%s.bin", i));
        Files.write(Ints.toByteArray(i), tmpFile);
        builder.add(String.valueOf(i), tmpFile);
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      Assertions.assertEquals(1, mapper.getSegmentFileMetadata().getContainers().size());
    }
  }

  private static void assertBytes(ByteBuffer actual, byte[] expected)
  {
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(expected.length, actual.remaining());
    final byte[] got = new byte[expected.length];
    actual.get(got);
    Assertions.assertArrayEquals(expected, got);
  }

  private static void assertNoContainerMixesProjections(SegmentFileMetadata metadata)
  {
    for (int containerIdx = 0; containerIdx < metadata.getContainers().size(); containerIdx++) {
      final Set<String> projectionsInContainer = new HashSet<>();
      for (Map.Entry<String, SegmentInternalFileMetadata> entry : metadata.getFiles().entrySet()) {
        if (entry.getValue().getContainer() == containerIdx) {
          final int slash = entry.getKey().indexOf('/');
          projectionsInContainer.add(slash < 0 ? "" : entry.getKey().substring(0, slash));
        }
      }
      Assertions.assertEquals(
          1,
          projectionsInContainer.size(),
          "container[" + containerIdx + "] mixes projections: " + projectionsInContainer
      );
    }
  }

  private static void assertColumns(String[] projections, int colCount, SegmentFileMapperV10 mapper) throws IOException
  {
    for (String projection : projections) {
      for (int col = 0; col < colCount; col++) {
        final String name = projection + "/col" + col;
        final ByteBuffer buf = mapper.mapFile(name);
        Assertions.assertNotNull(buf, name);
        Assertions.assertEquals(name.hashCode(), buf.getInt(), name);
      }
    }
  }
}
