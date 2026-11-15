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

package org.apache.druid.storage.s3;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.CloudObjectLocation;
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
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
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

public class S3GenerationLifecycleTest
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
    final InMemoryS3 s3 = new InMemoryS3();
    final S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket(BUCKET);
    config.setBaseKey("segments");
    final S3DataSegmentPusher pusher = new S3DataSegmentPusher(
        s3,
        config,
        new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP)
    );
    final File v10Source = IndexBuilder.create()
                                      .useV10()
                                      .tmpDir(new File(tempDir, "v10"))
                                      .rows(ImmutableList.of(new MapBasedInputRow(
                                          DateTimes.of("2015-01-01"),
                                          ImmutableList.of("dim"),
                                          ImmutableMap.of("dim", "a")
                                      )))
                                      .buildMMappedIndexFile();
    final int v10Version = SegmentUtils.getVersionFromDir(v10Source);
    final DataSegment v10Template = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "v10", 0))
                                               .loadSpec(new HashMap<>())
                                               .shardSpec(new NumberedShardSpec(0, 0))
                                               .binaryVersion(v10Version)
                                               .size(0)
                                               .build();

    final DataSegment pushedV10 = pusher.push(v10Source, v10Template, false);
    final String v10Path = loadSpecKey(pushedV10);
    assertEquals(IndexIO.V10_VERSION, v10Version);
    assertEquals(v10Version, (int) pushedV10.getBinaryVersion());
    assertTrue(v10Path.contains("/_druid_generations/"));
    assertFalse(v10Path.endsWith("index.zip"));
    assertTrue(v10Path.endsWith("/"));
    final List<String> v10Objects = s3.generationKeys(v10Path);
    assertFalse(v10Objects.isEmpty());
    assertTrue(v10Objects.stream().anyMatch(key -> key.endsWith("druid.segment")));
    assertTrue(v10Objects.stream().noneMatch(key -> key.endsWith("index.zip")));

    final File v9Source = IndexBuilder.create()
                                     .tmpDir(new File(tempDir, "v9"))
                                     .rows(ImmutableList.of(new MapBasedInputRow(
                                         DateTimes.of("2015-01-01"),
                                         ImmutableList.of("dim"),
                                         ImmutableMap.of("dim", "a")
                                     )))
                                     .buildMMappedIndexFile();
    final int v9Version = SegmentUtils.getVersionFromDir(v9Source);
    final DataSegment v9Template = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "v9", 0))
                                              .loadSpec(new HashMap<>())
                                              .shardSpec(new NumberedShardSpec(0, 0))
                                              .binaryVersion(v9Version)
                                              .size(0)
                                              .build();
    final DataSegment pushedV9 = pusher.push(v9Source, v9Template, false);
    assertEquals(v9Version, (int) pushedV9.getBinaryVersion());
    assertTrue(loadSpecKey(pushedV9).endsWith("index.zip"));
  }

  @Test
  public void testListingIsPaginatedAndRespectsGenerationBoundary() throws Exception
  {
    final InMemoryS3 s3 = new InMemoryS3();
    for (int i = 0; i < 5; i++) {
      s3.put(BUCKET, "_druid_generations/generation-a/file-" + i, new byte[]{(byte) i});
    }
    s3.put(BUCKET, "_druid_generations/generation-a-other/file", new byte[]{9});
    assertEquals(5, s3.listKeys(ListObjectsV2Request.builder()
                                                    .bucket(BUCKET)
                                                    .prefix("_druid_generations/generation-a/")
                                                    .maxKeys(2)
                                                    .build()).size());

    S3Utils.deleteObjectsInPath(s3, 2, BUCKET, "_druid_generations/generation-a", ignored -> true);
    assertTrue(s3.listKeys(ListObjectsV2Request.builder().bucket(BUCKET).prefix("_druid_generations/generation-a/").build()).isEmpty());
    assertEquals(List.of("_druid_generations/generation-a-other/file"), s3.listKeys(
        ListObjectsV2Request.builder().bucket(BUCKET).prefix("_druid_generations/generation-a-other/").build()
    ));
  }

  private static void runLifecycle(File tempDir, boolean zipFirst) throws Exception
  {
    final InMemoryS3 s3 = new InMemoryS3();
    final S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket(BUCKET);
    config.setBaseKey("segments");
    final DataSegment segment = DataSegment.builder(SegmentId.of("foo", Intervals.of("2015/2016"), "v", 0))
                                           .loadSpec(new HashMap<>())
                                           .shardSpec(new NumberedShardSpec(0, 0))
                                           .binaryVersion(1)
                                           .size(0)
                                           .build();
    final File source = IndexBuilder.create()
                                    .tmpDir(new File(tempDir, "source"))
                                    .rows(ImmutableList.of(new MapBasedInputRow(
                                        DateTimes.of("2015-01-01"),
                                        ImmutableList.of("dim"),
                                        ImmutableMap.of("dim", "a")
                                    )))
                                    .buildMMappedIndexFile();
    final S3DataSegmentPusher zipPusher = new S3DataSegmentPusher(s3, config, new DeepStorageSegmentConfig(true, CompressionUtils.Format.ZIP));
    final S3DataSegmentPusher noZipPusher = new S3DataSegmentPusher(s3, config, new DeepStorageSegmentConfig(false, CompressionUtils.Format.NONE));
    final DataSegment first = zipFirst ? zipPusher.push(source, segment, false) : noZipPusher.push(source, segment, false);
    final DataSegment second = zipFirst ? noZipPusher.push(source, segment, false) : zipPusher.push(source, segment, false);
    final String firstPath = loadSpecKey(first);
    final String secondPath = loadSpecKey(second);
    assertTrue(firstPath.contains("/_druid_generations/"));
    assertTrue(secondPath.contains("/_druid_generations/"));
    assertNotEquals(firstPath, secondPath);

    final S3DataSegmentPuller puller = new S3DataSegmentPuller(s3);
    assertPulledContents(source, puller, firstPath, new File(tempDir, "first"));
    assertPulledContents(source, puller, secondPath, new File(tempDir, "second"));
    final List<String> firstObjects = s3.generationKeys(firstPath);
    final List<String> secondObjects = s3.generationKeys(secondPath);
    new S3DataSegmentKiller(Suppliers.ofInstance(s3), config, new S3InputDataConfig()).kill(first);
    for (String key : firstObjects) {
      assertFalse(s3.contains(key));
    }
    for (String key : secondObjects) {
      assertTrue(s3.contains(key));
    }
    assertPulledContents(source, puller, secondPath, new File(tempDir, "second-after-kill"));
  }

  private static void assertPulledContents(File source, S3DataSegmentPuller puller, String key, File output) throws Exception
  {
    puller.getSegmentFiles(new CloudObjectLocation(BUCKET, key), output);
    final Map<String, Path> expected = files(source);
    final Map<String, Path> actual = files(output);
    assertEquals(expected.keySet(), actual.keySet());
    for (String relativePath : expected.keySet()) {
      assertArrayEquals(Files.readAllBytes(expected.get(relativePath)), Files.readAllBytes(actual.get(relativePath)));
    }
  }

  private static Map<String, Path> files(File dir) throws IOException
  {
    try (Stream<Path> paths = Files.walk(dir.toPath())) {
      return paths.filter(Files::isRegularFile)
                  .sorted()
                  .collect(Collectors.toMap(
                      path -> dir.toPath().relativize(path).toString(),
                      path -> path,
                      (left, ignored) -> left,
                      LinkedHashMap::new
                  ));
    }
  }

  private static String loadSpecKey(DataSegment segment)
  {
    return (String) Objects.requireNonNull(segment.getLoadSpec()).get("key");
  }

  private record ObjectKey(String bucket, String key) {}

  private static class InMemoryS3 extends ServerSideEncryptingAmazonS3
  {
    private final ConcurrentMap<ObjectKey, byte[]> objects = new ConcurrentHashMap<>();

    InMemoryS3()
    {
      super(null, null, null, null);
    }

    @Override
    public Grant getBucketOwnerGrant(String bucket)
    {
      return Grant.builder().build();
    }

    @Override
    public void upload(String bucket, String key, File file, Grant grant)
    {
      try {
        put(bucket, key, Files.readAllBytes(file.toPath()));
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    void put(String bucket, String key, byte[] bytes)
    {
      objects.put(new ObjectKey(bucket, key), bytes.clone());
    }

    @Override
    public ListObjectsV2Response listObjectsV2(ListObjectsV2Request request)
    {
      final String prefix = request.prefix() == null ? "" : request.prefix();
      final List<String> keys = objects.keySet().stream()
                                        .filter(k -> k.bucket.equals(request.bucket()) && k.key.startsWith(prefix))
                                        .map(k -> k.key)
                                        .sorted()
                                        .toList();
      final int start = request.continuationToken() == null
                        ? 0
                        : Collections.binarySearch(keys, request.continuationToken()) >= 0
                          ? Collections.binarySearch(keys, request.continuationToken()) + 1
                          : -Collections.binarySearch(keys, request.continuationToken()) - 1;
      final int maxKeys = request.maxKeys() == null ? keys.size() : request.maxKeys();
      final int end = Math.min(start + maxKeys, keys.size());
      final List<S3Object> page = keys.subList(start, end).stream()
                                       .map(key -> S3Object.builder().key(key).size((long) objects.get(new ObjectKey(request.bucket(), key)).length).build())
                                       .toList();
      return ListObjectsV2Response.builder()
                                  .contents(page)
                                  .isTruncated(end < keys.size())
                                  .nextContinuationToken(end < keys.size() ? keys.get(end - 1) : null)
                                  .build();
    }

    List<String> listKeys(ListObjectsV2Request request)
    {
      final ArrayList<String> result = new ArrayList<>();
      String token = request.continuationToken();
      do {
        final ListObjectsV2Response response = listObjectsV2(request.toBuilder().continuationToken(token).build());
        response.contents().forEach(object -> result.add(object.key()));
        token = response.nextContinuationToken();
      } while (token != null);
      return result;
    }

    List<String> generationKeys(String path)
    {
      final String prefix = path.endsWith("index.zip") ? path.substring(0, path.length() - "index.zip".length()) : path;
      return listKeys(ListObjectsV2Request.builder().bucket(BUCKET).prefix(prefix).build());
    }

    @Override
    public ResponseInputStream<GetObjectResponse> getObject(String bucket, String key)
    {
      final byte[] bytes = objects.get(new ObjectKey(bucket, key));
      if (bytes == null) {
        throw new IllegalStateException("Missing object " + bucket + "/" + key);
      }
      return new ResponseInputStream<>(GetObjectResponse.builder().build(), AbortableInputStream.create(new ByteArrayInputStream(bytes)));
    }

    @Override
    public boolean doesObjectExist(String bucket, String key)
    {
      return objects.containsKey(new ObjectKey(bucket, key));
    }

    @Override
    public void deleteObject(String bucket, String key)
    {
      objects.remove(new ObjectKey(bucket, key));
    }

    @Override
    public DeleteObjectsResponse deleteObjects(DeleteObjectsRequest request)
    {
      for (ObjectIdentifier object : request.delete().objects()) {
        deleteObject(request.bucket(), object.key());
      }
      return DeleteObjectsResponse.builder().build();
    }

    boolean contains(String key)
    {
      return doesObjectExist(BUCKET, key);
    }
  }
}
