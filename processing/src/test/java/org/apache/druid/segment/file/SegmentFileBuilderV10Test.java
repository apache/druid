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
import org.apache.druid.segment.CompressedPools;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnDescriptor;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.GenericIndexed;
import org.apache.druid.segment.data.GenericIndexedWriter;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMedium;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

class SegmentFileBuilderV10Test
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  @TempDir
  File tempDir;

  @Test
  void testOneContainerPerProjection() throws IOException
  {
    final File baseDir = newBaseDir();

    // matches the production usage pattern in IndexMergerV10: call startFileBundle then write that projection's
    // columns, then move on to the next projection.
    final String[] projections = {"__base", "projA", "projB"};
    final int colCount = 3;
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (String projection : projections) {
        builder.startFileBundle(projection);
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
    final File baseDir = newBaseDir();

    final String slashyProjection = "nested/projection";
    final int colCount = 3;
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("__base");
      for (int col = 0; col < colCount; col++) {
        final String name = "__base/col" + col;
        final File tmpFile = new File(tempDir, StringUtils.format("base-%s.bin", col));
        Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
        builder.add(name, tmpFile);
      }
      builder.startFileBundle(slashyProjection);
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
  void testAddWithoutGroupPrefixThrowsWhenGroupActive() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("projA");
      final File tmp = new File(tempDir, "no-prefix.bin");
      Files.write(Ints.toByteArray(1), tmp);
      // file name doesn't start with "projA/", so add must throw
      Assertions.assertThrows(RuntimeException.class, () -> builder.add("wrong/col0", tmp));
    }
  }

  @Test
  void testAddWithChannelWithoutGroupPrefixThrowsWhenGroupActive() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("projA");
      Assertions.assertThrows(RuntimeException.class, () -> builder.addWithChannel("wrong/col0", 4));
    }
  }

  @Test
  void testAddColumnWithoutGroupPrefixThrowsWhenGroupActive() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("projA");
      Assertions.assertThrows(
          RuntimeException.class,
          () -> builder.addColumn("wrong_no_prefix", new ColumnDescriptor.Builder()
              .setValueType(ValueType.LONG)
              .build())
      );
    }
  }

  @Test
  void testAddWithoutPrefixIsAllowedInRootBundle() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      // never call startFileBundle; bare names are fine under the default root bundle
      final File tmp = new File(tempDir, "bare.bin");
      Files.write(Ints.toByteArray(1), tmp);
      builder.add("col0", tmp);
    }
    // success: no exception
  }

  @Test
  void testContainerMetadataCarriesBundle() throws IOException
  {
    final File baseDir = newBaseDir();

    final String[] projections = {"__base", "projA", "projB"};
    final int colCount = 2;
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (String projection : projections) {
        builder.startFileBundle(projection);
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

      // Each container's bundle must equal the bundle active when it was written. Each container holds files from
      // exactly one bundle, so the first file's name prefix is authoritative.
      for (int ci = 0; ci < metadata.getContainers().size(); ci++) {
        final int containerIdx = ci;
        final String expectedBundle = metadata.getFiles().entrySet().stream()
            .filter(e -> e.getValue().getContainer() == containerIdx)
            .map(e -> e.getKey().substring(0, e.getKey().indexOf('/')))
            .findFirst()
            .orElseThrow();
        Assertions.assertEquals(
            expectedBundle,
            metadata.getContainers().get(ci).getBundle(),
            "container " + ci + " bundle mismatch"
        );
      }
    }
  }

  @Test
  void testContainerWrittenWithoutStartFileBundleDefaultsToRoot() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      // never call startFileBundle; the single container should be tagged with ROOT_BUNDLE_NAME
      for (int col = 0; col < 3; col++) {
        final String name = "col" + col;
        final File tmpFile = new File(tempDir, StringUtils.format("nobundle-%s.bin", col));
        Files.write(Ints.toByteArray(name.hashCode()), tmpFile);
        builder.add(name, tmpFile);
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      Assertions.assertEquals(1, metadata.getContainers().size());
      Assertions.assertEquals(
          SegmentFileBuilder.ROOT_BUNDLE_NAME,
          metadata.getContainers().get(0).getBundle()
      );
    }
  }

  @Test
  void testStartFileBundleNullResetsToRoot() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("first");
      final File firstFile = new File(tempDir, "first.bin");
      Files.write(Ints.toByteArray(1), firstFile);
      builder.add("first/a", firstFile);

      // Passing null resets to ROOT_BUNDLE_NAME; subsequent writes go in a root-bundle container.
      builder.startFileBundle(null);
      final File rootFile = new File(tempDir, "root.bin");
      Files.write(Ints.toByteArray(2), rootFile);
      builder.add("root_a", rootFile);
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      Assertions.assertEquals(2, metadata.getContainers().size());
      Assertions.assertEquals("first", metadata.getContainers().get(0).getBundle());
      Assertions.assertEquals(
          SegmentFileBuilder.ROOT_BUNDLE_NAME,
          metadata.getContainers().get(1).getBundle()
      );
    }
  }

  @Test
  void testStartFileBundleWhileWriterInUseThrows() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("__base");
      try (SegmentFileChannel outer = builder.addWithChannel("__base/col0", 4)) {
        Assertions.assertThrows(RuntimeException.class, () -> builder.startFileBundle("projA"));
        outer.write(ByteBuffer.wrap(new byte[]{1, 2, 3, 4}));
      }
    }
  }

  @Test
  void testStartFileBundleWithRootNameIsSameAsNull() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      // Explicit ROOT_BUNDLE_NAME and null are equivalent; both resolve to the default root bundle.
      builder.startFileBundle(SegmentFileBuilder.ROOT_BUNDLE_NAME);
      final File tmp = new File(baseDir, "tmp.bin");
      Files.write(new byte[]{1, 2, 3, 4}, tmp);
      builder.add("col0", tmp);
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      Assertions.assertEquals(1, metadata.getContainers().size());
      Assertions.assertEquals(
          SegmentFileBuilder.ROOT_BUNDLE_NAME,
          metadata.getContainers().get(0).getBundle()
      );
    }
  }

  @Test
  void testExternalBuilderAlsoSplitsContainersByProjection() throws IOException
  {
    final String externalName = "external.segment";
    final File baseDir = newBaseDir();

    final String[] mainProjections = {"__base", "projA", "projB"};
    final String[] externalProjections = {"extProjX", "extProjY"};
    final int colCount = 3;

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      for (String projection : mainProjections) {
        builder.startFileBundle(projection);
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
        external.startFileBundle(projection);
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
    final File baseDir = newBaseDir();

    final byte[] outerBytes = new byte[]{1, 2, 3, 4};
    final byte[] nestedBytes = new byte[]{5, 6, 7, 8};

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("__base");
      try (SegmentFileChannel outer = builder.addWithChannel("__base/outer", outerBytes.length)) {
        // nested write while outer is in use → forced into delegate temp file
        try (SegmentFileChannel nested = builder.addWithChannel("__base/nested", nestedBytes.length)) {
          nested.write(ByteBuffer.wrap(nestedBytes));
        }
        outer.write(ByteBuffer.wrap(outerBytes));
      }

      final SegmentFileBuilder external = builder.getExternalBuilder(externalName);
      external.startFileBundle("extProj");
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
  void testNestedDelegateClosedAfterOuterRoutesToOriginalBundle() throws IOException
  {
    // doing something like this is weird and probably should not happen in practice, but if a nested write was
    // requested while bundle "groupA" was active; even if the caller switches to "groupB" before finally closing the
    // nested channel, the delegated bytes must still land in groupA's container, not groupB's. Otherwise bundles break
    // and files from other bundles end up in the same container.
    final File baseDir = newBaseDir();

    final byte[] outerBytes = new byte[]{1, 2, 3, 4};
    final byte[] nestedBytes = new byte[]{5, 6, 7, 8};
    final byte[] groupBBytes = new byte[]{9, 10, 11, 12};

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("groupA");

      final SegmentFileChannel outer = builder.addWithChannel("groupA/outer", outerBytes.length);
      final SegmentFileChannel nested = builder.addWithChannel("groupA/nested", nestedBytes.length);
      nested.write(ByteBuffer.wrap(nestedBytes));

      // close the outer first so writerCurrentlyInUse clears while the nested delegate is still open
      outer.write(ByteBuffer.wrap(outerBytes));
      outer.close();

      // switch group before closing the still-open nested delegate; merge must use the snapshotted "groupA"
      builder.startFileBundle("groupB");
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
  void testColumnFilesAttribution() throws IOException
  {
    final File baseDir = newBaseDir();

    final byte[] bytes = new byte[]{1, 2, 3, 4};
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("__base");

      // a file written before any column scope is unattributed
      try (SegmentFileChannel preamble = builder.addWithChannel("__base/preamble", bytes.length)) {
        preamble.write(ByteBuffer.wrap(bytes));
      }

      // colA writes its main file plus a nested sub-file through a delegate (the production shape: sub-files are
      // opened while the outer writer holds the builder)
      builder.addColumn("__base/colA", longColumnDescriptor());
      try (SegmentFileChannel outer = builder.addWithChannel("__base/colA", bytes.length)) {
        try (SegmentFileChannel nested = builder.addWithChannel("__base/colA.__sub", bytes.length)) {
          nested.write(ByteBuffer.wrap(bytes));
        }
        outer.write(ByteBuffer.wrap(bytes));
      }

      // colB replaces colA's scope
      builder.addColumn("__base/colB", longColumnDescriptor());
      try (SegmentFileChannel outer = builder.addWithChannel("__base/colB", bytes.length)) {
        outer.write(ByteBuffer.wrap(bytes));
      }
      builder.finishColumn();

      // finishColumn closed colB's scope, so a trailing non-column file in the same bundle is unattributed
      try (SegmentFileChannel trailer = builder.addWithChannel("__base/trailer", bytes.length)) {
        trailer.write(ByteBuffer.wrap(bytes));
      }

      // a bundle transition closes the column scope, so projA/loose is unattributed
      builder.startFileBundle("projA");
      try (SegmentFileChannel loose = builder.addWithChannel("projA/loose", bytes.length)) {
        loose.write(ByteBuffer.wrap(bytes));
      }
      builder.addColumn("projA/colA", longColumnDescriptor());
      try (SegmentFileChannel outer = builder.addWithChannel("projA/colA", bytes.length)) {
        outer.write(ByteBuffer.wrap(bytes));
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final Map<String, List<String>> columnFiles = mapper.getSegmentFileMetadata().getColumnFiles();
      Assertions.assertNotNull(columnFiles);
      Assertions.assertEquals(
          Map.of(
              "__base/colA", List.of("__base/colA", "__base/colA.__sub"),
              "__base/colB", List.of("__base/colB"),
              "projA/colA", List.of("projA/colA")
          ),
          columnFiles
      );
    }
  }

  @Test
  void testColumnFilesDelegateClosedAfterColumnScopeChangeAttributedToSnapshotColumn() throws IOException
  {
    // delegate created under colA's scope but closed after the scope has moved on (even after a bundle switch) must
    // still be attributed to colA, mirroring how the delegate's bundle is snapshotted
    final File baseDir = newBaseDir();

    final byte[] bytes = new byte[]{1, 2, 3, 4};
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      builder.startFileBundle("groupA");
      builder.addColumn("groupA/colA", longColumnDescriptor());

      final SegmentFileChannel outer = builder.addWithChannel("groupA/colA", bytes.length);
      final SegmentFileChannel nested = builder.addWithChannel("groupA/colA.__sub", bytes.length);
      nested.write(ByteBuffer.wrap(bytes));

      outer.write(ByteBuffer.wrap(bytes));
      outer.close();

      // scope moves to another bundle + column before the delegate finally closes
      builder.startFileBundle("groupB");
      builder.addColumn("groupB/colB", longColumnDescriptor());
      nested.close();

      try (SegmentFileChannel other = builder.addWithChannel("groupB/colB", bytes.length)) {
        other.write(ByteBuffer.wrap(bytes));
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final Map<String, List<String>> columnFiles = mapper.getSegmentFileMetadata().getColumnFiles();
      Assertions.assertNotNull(columnFiles);
      Assertions.assertEquals(List.of("groupA/colA", "groupA/colA.__sub"), columnFiles.get("groupA/colA"));
      Assertions.assertEquals(List.of("groupB/colB"), columnFiles.get("groupB/colB"));
    }
  }

  @Test
  void testColumnFilesExternalBuilderAttribution() throws IOException
  {
    // a column scope opened on the main builder broadcasts to externals, so files a serde writes into an external
    // land in the external's own columnFiles under the same column
    final String externalName = "external.segment";
    final File baseDir = newBaseDir();

    final byte[] bytes = new byte[]{1, 2, 3, 4};
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      // external created before the column scope opens; scope must broadcast on addColumn
      final SegmentFileBuilder external = builder.getExternalBuilder(externalName);

      builder.addColumn("colA", longColumnDescriptor());
      try (SegmentFileChannel main = builder.addWithChannel("colA", bytes.length)) {
        main.write(ByteBuffer.wrap(bytes));
      }
      try (SegmentFileChannel ext = external.addWithChannel("colA.external", bytes.length)) {
        ext.write(ByteBuffer.wrap(bytes));
      }
    }

    final File externalFile = new File(baseDir, externalName);
    try (SegmentFileMapperV10 externalOnly = SegmentFileMapperV10.create(externalFile, JSON_MAPPER)) {
      Assertions.assertEquals(
          Map.of("colA", List.of("colA.external")),
          externalOnly.getSegmentFileMetadata().getColumnFiles()
      );
    }
    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      Assertions.assertEquals(
          Map.of("colA", List.of("colA")),
          mapper.getSegmentFileMetadata().getColumnFiles()
      );
    }
  }

  @Test
  void testColumnFilesAbsentWhenNoColumnsDeclared() throws IOException
  {
    final File baseDir = newBaseDir();

    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      final File tmpFile = new File(tempDir, "no-columns.bin");
      Files.write(Ints.toByteArray(1), tmpFile);
      builder.add("plain", tmpFile);
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      Assertions.assertNull(mapper.getSegmentFileMetadata().getColumnFiles());
    }
  }

  @Test
  void testColumnFilesGenericIndexedMultiFileSplitAttributed() throws IOException
  {
    // a GenericIndexedWriter forced into multi-file (version 2) mode emits <base>_header and <base>_value_N files
    // through the builder while the column's outer channel is open; all of them must be attributed to the column
    final File baseDir = newBaseDir();

    final String columnName = "colA";
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir)) {
      final GenericIndexedWriter<String> writer = new GenericIndexedWriter<>(
          new OnHeapMemorySegmentWriteOutMedium(),
          columnName,
          GenericIndexed.STRING_STRATEGY,
          1 << 16
      );
      writer.open();
      for (int i = 0; i < 20_000; i++) {
        writer.write(StringUtils.format("a_reasonably_long_dictionary_value_%05d", i));
      }
      builder.addColumn(columnName, longColumnDescriptor());
      try (SegmentFileChannel channel = builder.addWithChannel(columnName, writer.getSerializedSize())) {
        writer.writeTo(channel, builder);
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
      Assertions.assertNotNull(columnFiles);
      final List<String> files = columnFiles.get(columnName);
      Assertions.assertNotNull(files);
      Assertions.assertEquals(columnName, files.get(0));
      Assertions.assertTrue(
          files.contains(GenericIndexedWriter.generateHeaderFileName(columnName)),
          "missing header file in " + files
      );
      Assertions.assertTrue(
          files.contains(GenericIndexedWriter.generateValueFileName(columnName, 0)),
          "missing first value file in " + files
      );
      Assertions.assertEquals(metadata.getFiles().keySet(), new TreeSet<>(files));
    }
  }

  private static ColumnDescriptor longColumnDescriptor()
  {
    return new ColumnDescriptor.Builder().setValueType(ValueType.LONG).build();
  }

  @Test
  void testUnprefixedFilesShareSingleContainer() throws IOException
  {
    final File baseDir = newBaseDir();

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

  @ParameterizedTest
  @EnumSource(CompressionStrategy.class)
  void testLargeMetadataRoundTrips(CompressionStrategy metaCompression) throws IOException
  {
    final File baseDir = newBaseDir();

    // bug fix test: build metadata large enough to exceed the fixed 64k pooled decompression buffer
    // (CompressedPools.BUFFER_SIZE) used by the default metadata compression (zstd) when using heap buffers.
    final int fileCount = 2000;
    final byte[] payload = Ints.toByteArray(0xCAFEBABE);
    final File payloadFile = new File(tempDir, "payload-" + metaCompression + ".bin");
    Files.write(payload, payloadFile);

    final Set<String> expectedNames = new TreeSet<>();
    try (SegmentFileBuilderV10 builder = SegmentFileBuilderV10.create(JSON_MAPPER, baseDir, metaCompression)) {
      for (int i = 0; i < fileCount; i++) {
        final String name = StringUtils.format("a_reasonably_long_internal_file_name_for_padding_metadata_%05d", i);
        expectedNames.add(name);
        builder.add(name, payloadFile);
      }
    }

    final File segmentFile = new File(baseDir, IndexIO.V10_FILE_NAME);

    final int uncompressedMetaLength = readUncompressedMetaLength(segmentFile);
    Assertions.assertTrue(
        uncompressedMetaLength > CompressedPools.BUFFER_SIZE,
        StringUtils.format(
            "metadata length[%,d] must exceed the pooled buffer size[%,d] to exercise the fallback",
            uncompressedMetaLength,
            CompressedPools.BUFFER_SIZE
        )
    );

    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      Assertions.assertEquals(fileCount, metadata.getFiles().size());
      Assertions.assertEquals(expectedNames, metadata.getFiles().keySet());

      // spot-check that a few files (first, middle, last) still round-trip their content
      for (int i : new int[]{0, fileCount / 2, fileCount - 1}) {
        final String name = StringUtils.format("a_reasonably_long_internal_file_name_for_padding_metadata_%05d", i);
        assertBytes(mapper.mapFile(name), payload);
      }
    }
  }

  private static int readUncompressedMetaLength(File segmentFile) throws IOException
  {
    final byte[] header = new byte[SegmentFileMetadataReader.HEADER_SIZE];
    try (FileInputStream fis = new FileInputStream(segmentFile)) {
      int offset = 0;
      while (offset < header.length) {
        final int read = fis.read(header, offset, header.length - offset);
        if (read < 0) {
          break;
        }
        offset += read;
      }
    }
    return ByteBuffer.wrap(header).order(ByteOrder.LITTLE_ENDIAN).getInt(2);
  }

  private static void assertBytes(ByteBuffer actual, byte[] expected)
  {
    Assertions.assertNotNull(actual);
    Assertions.assertEquals(expected.length, actual.remaining());
    final byte[] got = new byte[expected.length];
    actual.get(got);
    Assertions.assertArrayEquals(expected, got);
  }

  private File newBaseDir() throws IOException
  {
    final File baseDir = new File(tempDir, "base_" + ThreadLocalRandom.current().nextInt());
    FileUtils.mkdirp(baseDir);
    return baseDir;
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
