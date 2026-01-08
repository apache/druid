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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ObjectSummaryIteratorTest
{
  // Store bucket with each object for testing
  private static final String TEST_BUCKET = "b";
  private static final ImmutableList<Map.Entry<String, S3Object>> TEST_OBJECTS =
      ImmutableList.of(
          makeObjectEntry(TEST_BUCKET, "foo", 10L),
          makeObjectEntry(TEST_BUCKET, "foo/", 0L), // directory
          makeObjectEntry(TEST_BUCKET, "foo/bar1", 10L),
          makeObjectEntry(TEST_BUCKET, "foo/bar2", 10L),
          makeObjectEntry(TEST_BUCKET, "foo/bar3", 10L),
          makeObjectEntry(TEST_BUCKET, "foo/bar4", 10L),
          makeObjectEntry(TEST_BUCKET, "foo/bar5", 0L), // empty object
          makeObjectEntry(TEST_BUCKET, "foo/baz", 10L),
          makeObjectEntry("bucketnotmine", "a/different/bucket", 10L),
          makeObjectEntry(TEST_BUCKET, "foo/bar/", 0L) // another directory at the end of list
      );

  @Test
  public void testSingleObject()
  {
    test(
        ImmutableList.of("s3://b/foo/baz"),
        ImmutableList.of("s3://b/foo/baz"),
        5
    );
  }

  @Test
  public void testMultiObjectOneKeyAtATime()
  {
    test(
        ImmutableList.of("s3://b/foo/bar1", "s3://b/foo/bar2", "s3://b/foo/bar3", "s3://b/foo/bar4", "s3://b/foo/baz"),
        ImmutableList.of("s3://b/foo/"),
        1
    );
  }

  @Test
  public void testMultiObjectTwoKeysAtATime()
  {
    test(
        ImmutableList.of("s3://b/foo/bar1", "s3://b/foo/bar2", "s3://b/foo/bar3", "s3://b/foo/bar4", "s3://b/foo/baz"),
        ImmutableList.of("s3://b/foo/"),
        2
    );
  }

  @Test
  public void testMultiObjectTenKeysAtATime()
  {
    test(
        ImmutableList.of("s3://b/foo/bar1", "s3://b/foo/bar2", "s3://b/foo/bar3", "s3://b/foo/bar4", "s3://b/foo/baz"),
        ImmutableList.of("s3://b/foo/"),
        10
    );
  }

  @Test
  public void testPrefixInMiddleOfKey()
  {
    test(
        ImmutableList.of("s3://b/foo/bar1", "s3://b/foo/bar2", "s3://b/foo/bar3", "s3://b/foo/bar4"),
        ImmutableList.of("s3://b/foo/bar"),
        10
    );
  }

  @Test
  public void testNoPath()
  {
    test(
        ImmutableList.of(
            "s3://b/foo",
            "s3://b/foo/bar1",
            "s3://b/foo/bar2",
            "s3://b/foo/bar3",
            "s3://b/foo/bar4",
            "s3://b/foo/baz"
        ),
        ImmutableList.of("s3://b"),
        10
    );
  }

  @Test
  public void testSlashPath()
  {
    test(
        ImmutableList.of(
            "s3://b/foo",
            "s3://b/foo/bar1",
            "s3://b/foo/bar2",
            "s3://b/foo/bar3",
            "s3://b/foo/bar4",
            "s3://b/foo/baz"
        ),
        ImmutableList.of("s3://b/"),
        10
    );
  }

  @Test
  public void testDifferentBucket()
  {
    test(
        ImmutableList.of(),
        ImmutableList.of("s3://bx/foo/"),
        10
    );
  }

  @Test
  public void testWithMultiplePrefixesReturningAllNonEmptyObjectsStartingWithOneOfPrefixes()
  {
    test(
        ImmutableList.of("s3://b/foo/bar1", "s3://b/foo/bar2", "s3://b/foo/bar3", "s3://b/foo/bar4", "s3://b/foo/baz"),
        ImmutableList.of("s3://b/foo/bar", "s3://b/foo/baz"),
        10
    );
  }

  private static void test(
      final List<String> expectedUris,
      final List<String> prefixes,
      final int maxListingLength
  )
  {
    final List<S3Object> expectedObjects = new ArrayList<>();

    // O(N^2) but who cares -- the list is short.
    for (final String uri : expectedUris) {
      final List<S3Object> matches = TEST_OBJECTS.stream()
                                                        .filter(
                                                            entry ->
                                                                S3Utils.summaryToUri(entry.getValue(), entry.getKey()).toString().equals(uri)
                                                        )
                                                        .map(Map.Entry::getValue)
                                                        .collect(Collectors.toList());

      expectedObjects.add(Iterables.getOnlyElement(matches));
    }

    final List<S3Object> actualObjects = ImmutableList.copyOf(
        S3Utils.objectSummaryIterator(
            makeMockClient(TEST_OBJECTS),
            prefixes.stream().map(URI::create).collect(Collectors.toList()),
            maxListingLength
        )
    );

    // For comparison, we need to extract bucket from prefix since S3Object doesn't have bucket
    // Using the first prefix's bucket for comparison
    final String bucketForComparison = prefixes.isEmpty() ? TEST_BUCKET : URI.create(prefixes.get(0)).getAuthority();

    Assert.assertEquals(
        prefixes.toString(),
        expectedObjects.stream().map(obj -> S3Utils.summaryToUri(obj, bucketForComparison)).collect(Collectors.toList()),
        actualObjects.stream().map(obj -> S3Utils.summaryToUri(obj, bucketForComparison)).collect(Collectors.toList())
    );
  }

  /**
   * Makes a mock S3 client that handles enough of "listObjectsV2" to test the functionality of the
   * {@link ObjectSummaryIterator} class.
   */
  private static ServerSideEncryptingAmazonS3 makeMockClient(
      final List<Map.Entry<String, S3Object>> objects
  )
  {
    return new ServerSideEncryptingAmazonS3(null, null, new S3TransferConfig())
    {
      @Override
      public ListObjectsV2Response listObjectsV2(final ListObjectsV2Request request)
      {
        // Continuation token is an index in the "objects" list.
        final String continuationToken = request.continuationToken();
        final int startIndex = continuationToken == null ? 0 : Integer.parseInt(continuationToken);

        // Find matching objects.
        final List<S3Object> summaries = new ArrayList<>();
        int nextIndex = -1;

        for (int i = startIndex; i < objects.size(); i++) {
          final Map.Entry<String, S3Object> entry = objects.get(i);
          final String bucket = entry.getKey();
          final S3Object summary = entry.getValue();

          if (bucket.equals(request.bucket())
              && summary.key().startsWith(request.prefix())) {

            if (summaries.size() == request.maxKeys()) {
              // We reached our max key limit; set nextIndex (which will lead to a result with truncated = true).
              nextIndex = i;
              break;
            }

            // Generate a summary.
            summaries.add(summary);
          }
        }

        // Generate the result.
        ListObjectsV2Response.Builder builder = ListObjectsV2Response.builder()
            .contents(summaries);

        if (nextIndex >= 0) {
          builder.isTruncated(true);
          builder.nextContinuationToken(String.valueOf(nextIndex));
        } else {
          builder.isTruncated(false);
        }

        return builder.build();
      }
    };
  }

  private static Map.Entry<String, S3Object> makeObjectEntry(final String bucket, final String key, final long size)
  {
    final S3Object summary = S3Object.builder()
        .key(key)
        .size(size)
        .build();
    return new AbstractMap.SimpleEntry<>(bucket, summary);
  }
}
