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

package org.apache.druid.storage.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AzureGenerationLifecycleTest
{
  private static final String CONTAINER = "container";

  @Test
  public void testZipAndUncompressedGenerationsRemainIndependent(@TempDir File tempDir) throws Exception
  {
    runLifecycle(tempDir, true);
  }

  @Test
  public void testUncompressedAndZipGenerationsRemainIndependent(@TempDir File tempDir) throws Exception
  {
    runLifecycle(tempDir, false);
  }

  @Test
  public void testV10BypassesConfiguredZipCompression(@TempDir File tempDir) throws Exception
  {
    final InMemoryAzureStorage storage = new InMemoryAzureStorage();
    final AzureAccountConfig accountConfig = accountConfig();
    final AzureDataSegmentConfig segmentConfig = segmentConfig();
    final AzureDataSegmentPusher pusher = new AzureDataSegmentPusher(
        storage,
        accountConfig,
        segmentConfig,
        new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP)
    );
    final File v10Source = buildSegment(tempDir, "v10", true);
    final File v9Source = buildSegment(tempDir, "v9", false);
    final DataSegment v10Template = template("v10", SegmentUtils.getVersionFromDir(v10Source));
    final DataSegment v9Template = template("v9", SegmentUtils.getVersionFromDir(v9Source));

    final DataSegment v10 = pusher.push(v10Source, v10Template, false);
    final String v10Path = path(v10);
    assertEquals(IndexIO.V10_VERSION, (int) v10.getBinaryVersion());
    assertTrue(v10Path.endsWith("/"));
    assertTrue(storage.keys(CONTAINER, v10Path).stream().anyMatch(key -> key.endsWith("druid.segment")));
    assertTrue(storage.keys(CONTAINER, v10Path).stream().noneMatch(key -> key.endsWith("index.zip")));

    final DataSegment v9 = pusher.push(v9Source, v9Template, false);
    final String v9Path = path(v9);
    assertTrue(v9Path.endsWith("index.zip"));
    assertTrue(storage.contains(v9Path));
  }

  @Test
  public void testDeletionRespectsGenerationBoundary()
  {
    for (String prefix : List.of("_druid_generations/generation-a", "_druid_generations/generation-a/")) {
      final InMemoryAzureStorage storage = new InMemoryAzureStorage();
      for (int i = 0; i < 5; i++) {
        storage.put(CONTAINER, "_druid_generations/generation-a/file-" + i, new byte[]{(byte) i});
      }
      storage.put(CONTAINER, "_druid_generations/generation-a-other/file", new byte[]{9});

      // The third argument is maxAttempts, not a page size. Pagination is owned by the Azure SDK PagedIterable.
      storage.emptyCloudBlobDirectory(CONTAINER, prefix, null);
      assertTrue(storage.keys(CONTAINER, "_druid_generations/generation-a/").isEmpty());
      assertEquals(
          List.of("_druid_generations/generation-a-other/file"),
          storage.keys(CONTAINER, "_druid_generations/generation-a-other/")
      );
    }
  }

  private static void runLifecycle(File tempDir, boolean zipFirst) throws Exception
  {
    final InMemoryAzureStorage storage = new InMemoryAzureStorage();
    final AzureAccountConfig accountConfig = accountConfig();
    final AzureDataSegmentConfig segmentConfig = segmentConfig();
    final File source = buildSegment(tempDir, "source", false);
    final DataSegment template = template("segment", SegmentUtils.getVersionFromDir(source));
    final AzureDataSegmentPusher zipPusher = new AzureDataSegmentPusher(
        storage,
        accountConfig,
        segmentConfig,
        new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP)
    );
    final AzureDataSegmentPusher noZipPusher = new AzureDataSegmentPusher(
        storage,
        accountConfig,
        segmentConfig,
        new DeepStorageSegmentConfig(false, CompressionUtils.Format.NONE)
    );
    final DataSegment first = zipFirst ? zipPusher.push(source, template, false) : noZipPusher.push(source, template, false);
    final DataSegment second = zipFirst ? noZipPusher.push(source, template, false) : zipPusher.push(source, template, false);
    final String firstPath = path(first);
    final String secondPath = path(second);
    assertTrue(firstPath.contains("/_druid_generations/"));
    assertTrue(secondPath.contains("/_druid_generations/"));
    assertNotEquals(firstPath, secondPath);

    final AzureDataSegmentPuller puller = new AzureDataSegmentPuller(
        (container, blob, azureStorage) -> new AzureByteSource(azureStorage, container, blob),
        storage,
        accountConfig
    );
    assertPulledContents(source, puller, firstPath, new File(tempDir, "first"));
    assertPulledContents(source, puller, secondPath, new File(tempDir, "second"));
    final List<String> firstObjects = storage.keys(CONTAINER, generationPrefix(firstPath));
    final List<String> secondObjects = storage.keys(CONTAINER, generationPrefix(secondPath));

    new AzureDataSegmentKiller(segmentConfig, new AzureInputDataConfig(), accountConfig, storage, null).kill(first);
    for (String key : firstObjects) {
      assertFalse(storage.contains(key));
    }
    for (String key : secondObjects) {
      assertTrue(storage.contains(key));
    }
    assertPulledContents(source, puller, secondPath, new File(tempDir, "second-after-kill"));
  }

  private static File buildSegment(File tempDir, String name, boolean v10)
  {
    final IndexBuilder builder = IndexBuilder.create()
                                             .tmpDir(new File(tempDir, name))
                                             .rows(ImmutableList.of(new MapBasedInputRow(
                                                 DateTimes.of("2015-01-01"),
                                                 ImmutableList.of("dim"),
                                                 ImmutableMap.of("dim", "a")
                                             )));
    return (v10 ? builder.useV10() : builder).buildMMappedIndexFile();
  }

  private static AzureAccountConfig accountConfig()
  {
    final AzureAccountConfig config = new AzureAccountConfig();
    config.setMaxTries(3);
    return config;
  }

  private static AzureDataSegmentConfig segmentConfig()
  {
    final AzureDataSegmentConfig config = new AzureDataSegmentConfig();
    config.setContainer(CONTAINER);
    config.setPrefix("segments");
    return config;
  }

  private static DataSegment template(String version, int binaryVersion)
  {
    return DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), version, 0))
                      .loadSpec(new HashMap<>())
                      .shardSpec(new NumberedShardSpec(0, 0))
                      .binaryVersion(binaryVersion)
                      .size(0)
                      .build();
  }

  private static String path(DataSegment segment)
  {
    return (String) Objects.requireNonNull(segment.getLoadSpec()).get("blobPath");
  }

  private static String generationPrefix(String path)
  {
    return path.endsWith("index.zip") ? path.substring(0, path.length() - "index.zip".length()) : path;
  }

  private static void assertPulledContents(
      File source,
      AzureDataSegmentPuller puller,
      String path,
      File output
  ) throws Exception
  {
    puller.getSegmentFiles(CONTAINER, path, output);
    final Map<String, Path> expected = files(source);
    final Map<String, Path> actual = files(output);
    assertEquals(expected.keySet(), actual.keySet());
    for (String relativePath : expected.keySet()) {
      assertArrayEquals(Files.readAllBytes(expected.get(relativePath)), Files.readAllBytes(actual.get(relativePath)));
    }
  }

  private static Map<String, Path> files(File directory) throws IOException
  {
    try (Stream<Path> paths = Files.walk(directory.toPath())) {
      return paths.filter(Files::isRegularFile)
                  .sorted()
                  .collect(Collectors.toMap(
                      path -> directory.toPath().relativize(path).toString(),
                      path -> path,
                      (left, ignored) -> left,
                      LinkedHashMap::new
                  ));
    }
  }

  private record ObjectKey(String container, String blob) {}

  private static final class InMemoryAzureStorage extends AzureStorage
  {
    private final ConcurrentMap<ObjectKey, byte[]> blobs = new ConcurrentHashMap<>();

    private InMemoryAzureStorage()
    {
      super(null, null);
    }

    @Override
    public void uploadBlockBlob(File file, String containerName, String blobName, Integer maxAttempts) throws IOException
    {
      put(containerName, blobName, Files.readAllBytes(file.toPath()));
    }

    @Override
    public InputStream getBlockBlobInputStream(long offset, String containerName, String blobName) throws BlobStorageException
    {
      final byte[] bytes = blobs.get(new ObjectKey(containerName, blobName));
      if (bytes == null) {
        throw new IllegalStateException("Missing blob " + containerName + "/" + blobName);
      }
      final ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes.clone());

      long skipped = inputStream.skip(offset);
      if (skipped != offset) {
        throw new IllegalStateException("Failed to skip requested offset: " + offset);
      }

      return inputStream;
    }

    @Override
    public List<String> listBlobs(String containerName, String prefix, Integer maxResults, Integer maxAttempts)
    {
      final List<String> matching = keys(containerName, prefix == null ? "" : prefix);
      return maxResults == null ? matching : matching.subList(0, Math.min(maxResults, matching.size()));
    }

    @Override
    public List<String> emptyCloudBlobDirectory(String containerName, String prefix, Integer maxAttempts)
    {
      final String normalizedPrefix = prefix == null || prefix.endsWith("/") ? prefix : prefix + "/";
      final List<String> matching = blobs.keySet().stream()
                                         .filter(key -> key.container.equals(containerName)
                                                        && (normalizedPrefix == null || key.blob.startsWith(normalizedPrefix)))
                                         .map(key -> key.blob)
                                         .sorted()
                                         .toList();
      matching.forEach(blob -> blobs.remove(new ObjectKey(containerName, blob)));
      return matching;
    }

    void put(String containerName, String blobName, byte[] bytes)
    {
      blobs.put(new ObjectKey(containerName, blobName), bytes.clone());
    }

    boolean contains(String blobName)
    {
      return blobs.containsKey(new ObjectKey(AzureGenerationLifecycleTest.CONTAINER, blobName));
    }

    List<String> keys(String containerName, String prefix)
    {
      return blobs.keySet().stream()
                 .filter(key -> key.container.equals(containerName) && key.blob.startsWith(prefix))
                 .map(key -> key.blob)
                 .sorted()
                 .toList();
    }
  }
}
