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

package org.apache.druid.storage.google;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.common.base.Suppliers;
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
import java.util.Collections;
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

public class GoogleGenerationLifecycleTest
{
  private static final String BUCKET = "bucket";

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
    final InMemoryGoogleStorage storage = new InMemoryGoogleStorage();
    final GoogleAccountConfig accountConfig = accountConfig();
    final GoogleInputDataConfig inputConfig = new GoogleInputDataConfig();
    final GoogleDataSegmentPusher pusher = new GoogleDataSegmentPusher(
        storage,
        accountConfig,
        inputConfig,
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
    assertTrue(storage.keys(v10Path).stream().anyMatch(key -> key.endsWith("druid.segment")));
    assertTrue(storage.keys(v10Path).stream().noneMatch(key -> key.endsWith("index.zip")));

    final DataSegment v9 = pusher.push(v9Source, v9Template, false);
    final String v9Path = path(v9);
    assertTrue(v9Path.endsWith("index.zip"));
    assertTrue(storage.contains(v9Path), "Expected ZIP object to exist: " + v9Path);
  }

  @Test
  public void testPaginatedDeletionRespectsGenerationBoundary() throws Exception
  {
    final InMemoryGoogleStorage storage = new InMemoryGoogleStorage();
    final GoogleInputDataConfig config = new GoogleInputDataConfig();
    config.setMaxListingLength(2);
    for (int i = 0; i < 5; i++) {
      storage.put(BUCKET, "_druid_generations/generation-a/file-" + i, new byte[]{(byte) i});
    }
    storage.put(BUCKET, "_druid_generations/generation-a-other/file", new byte[]{9});

    GoogleUtils.deleteObjectsInPath(
        storage,
        config,
        BUCKET,
        "_druid_generations/generation-a",
        ignored -> true
    );
    assertTrue(storage.keys("_druid_generations/generation-a/").isEmpty());
    assertEquals(List.of("_druid_generations/generation-a-other/file"), storage.keys("_druid_generations/generation-a-other/"));
  }

  private static void runLifecycle(File tempDir, boolean zipFirst) throws Exception
  {
    final InMemoryGoogleStorage storage = new InMemoryGoogleStorage();
    final GoogleAccountConfig accountConfig = accountConfig();
    final GoogleInputDataConfig inputConfig = new GoogleInputDataConfig();
    final File source = buildSegment(tempDir, "source", false);
    final DataSegment template = template("segment", SegmentUtils.getVersionFromDir(source));
    final GoogleDataSegmentPusher zipPusher = new GoogleDataSegmentPusher(
        storage,
        accountConfig,
        inputConfig,
        new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP)
    );
    final GoogleDataSegmentPusher noZipPusher = new GoogleDataSegmentPusher(
        storage,
        accountConfig,
        inputConfig,
        new DeepStorageSegmentConfig(false, CompressionUtils.Format.NONE)
    );
    final DataSegment first = zipFirst ? zipPusher.push(source, template, false) : noZipPusher.push(source, template, false);
    final DataSegment second = zipFirst ? noZipPusher.push(source, template, false) : zipPusher.push(source, template, false);
    final String firstPath = path(first);
    final String secondPath = path(second);
    assertTrue(firstPath.contains("/_druid_generations/"));
    assertTrue(secondPath.contains("/_druid_generations/"));
    assertNotEquals(firstPath, secondPath);

    final GoogleDataSegmentPuller puller = new GoogleDataSegmentPuller(storage, inputConfig);
    assertPulledContents(source, puller, firstPath, new File(tempDir, "first"));
    assertPulledContents(source, puller, secondPath, new File(tempDir, "second"));
    final List<String> firstObjects = storage.keys(generationPrefix(firstPath));
    final List<String> secondObjects = storage.keys(generationPrefix(secondPath));

    new GoogleDataSegmentKiller(storage, accountConfig, inputConfig).kill(first);
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

  private static GoogleAccountConfig accountConfig()
  {
    final GoogleAccountConfig config = new GoogleAccountConfig();
    config.setBucket(BUCKET);
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
    return (String) Objects.requireNonNull(segment.getLoadSpec()).get("path");
  }

  private static String generationPrefix(String path)
  {
    return path.endsWith("index.zip") ? path.substring(0, path.length() - "index.zip".length()) : path;
  }

  private static void assertPulledContents(
      File source,
      GoogleDataSegmentPuller puller,
      String path,
      File output
  ) throws Exception
  {
    puller.getSegmentFiles(BUCKET, path, output);
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

  private record ObjectKey(String bucket, String key) {}

  private static final class InMemoryGoogleStorage extends GoogleStorage
  {
    private final ConcurrentMap<ObjectKey, byte[]> objects = new ConcurrentHashMap<>();

    private InMemoryGoogleStorage()
    {
      super(Suppliers.ofInstance(null));
    }

    @Override
    public void insert(String bucket, String path, AbstractInputStreamContent content, Integer bufferSize) throws IOException
    {
      try (InputStream inputStream = content.getInputStream()) {
        put(bucket, path, inputStream.readAllBytes());
      }
    }

    @Override
    public InputStream getInputStream(String bucket, String path) throws IOException
    {
      final byte[] bytes = objects.get(new ObjectKey(bucket, path));
      if (bytes == null) {
        throw new IOException("Missing object " + bucket + "/" + path);
      }
      return new ByteArrayInputStream(bytes.clone());
    }

    @Override
    public GoogleStorageObjectPage list(String bucket, String prefix, Long pageSize, String pageToken)
    {
      final List<String> matchingKeys = objects.keySet().stream()
                                                .filter(key -> key.bucket.equals(bucket) && key.key.startsWith(prefix == null ? "" : prefix))
                                                .map(key -> key.key)
                                                .sorted()
                                                .toList();
      final int start;
      if (pageToken == null) {
        start = 0;
      } else {
        final int pageIndex = Collections.binarySearch(matchingKeys, pageToken);
        start = pageIndex >= 0 ? pageIndex + 1 : -pageIndex - 1;
      }
      final int end = pageSize == null ? matchingKeys.size() : Math.min(start + pageSize.intValue(), matchingKeys.size());
      final List<GoogleStorageObjectMetadata> page = matchingKeys.subList(start, end).stream()
                                                                  .map(key -> new GoogleStorageObjectMetadata(
                                                                      bucket,
                                                                      key,
                                                                      (long) objects.get(new ObjectKey(bucket, key)).length,
                                                                      0L
                                                                  ))
                                                                  .toList();
      return new GoogleStorageObjectPage(page, end < matchingKeys.size() ? matchingKeys.get(end - 1) : null);
    }

    @Override
    public void delete(String bucket, String path)
    {
      objects.remove(new ObjectKey(bucket, path));
    }

    void put(String bucket, String key, byte[] bytes)
    {
      objects.put(new ObjectKey(bucket, key), bytes.clone());
    }

    boolean contains(String key)
    {
      return objects.containsKey(new ObjectKey(GoogleGenerationLifecycleTest.BUCKET, key));
    }

    List<String> keys(String prefix)
    {
      return objects.keySet().stream()
                    .filter(key -> BUCKET.equals(key.bucket) && key.key.startsWith(prefix))
                    .map(key -> key.key)
                    .sorted()
                    .toList();
    }
  }
}
