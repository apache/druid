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

import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectSummaryIteratorTest
{
  private static final ImmutableList<S3ObjectSummary> TEST_OBJECTS =
      ImmutableList.of(
          makeObjectSummary("b", "foo", 10L),
          makeObjectSummary("b", "foo/", 0L), // directory
          makeObjectSummary("b", "foo/bar1", 10L),
          makeObjectSummary("b", "foo/bar2", 10L),
          makeObjectSummary("b", "foo/bar3", 10L),
          makeObjectSummary("b", "foo/bar4", 10L),
          makeObjectSummary("b", "foo/bar5", 0L), // empty object
          makeObjectSummary("b", "foo/baz", 10L),
          makeObjectSummary("bucketnotmine", "a/different/bucket", 10L),
          makeObjectSummary("b", "foo/bar/", 0L) // another directory at the end of list
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
    final List<S3ObjectSummary> expectedObjects = new ArrayList<>();

    // O(N^2) but who cares -- the list is short.
    for (final String uri : expectedUris) {
      final List<S3ObjectSummary> matches = TEST_OBJECTS.stream()
                                                        .filter(
                                                            summary ->
                                                                S3Utils.summaryToUri(summary).toString().equals(uri)
                                                        )
                                                        .collect(Collectors.toList());

      expectedObjects.add(Iterables.getOnlyElement(matches));
    }

    final List<S3ObjectSummary> actualObjects = ImmutableList.copyOf(
        S3Utils.objectSummaryIterator(
            makeMockClient(TEST_OBJECTS),
            prefixes.stream().map(URI::create).collect(Collectors.toList()),
            maxListingLength
        )
    );

    Assert.assertEquals(
        prefixes.toString(),
        expectedObjects.stream().map(S3Utils::summaryToUri).collect(Collectors.toList()),
        actualObjects.stream().map(S3Utils::summaryToUri).collect(Collectors.toList())
    );
  }

  /**
   * Makes a mock S3 client that handles enough of "listObjectsV2" to test the functionality of the
   * {@link ObjectSummaryIterator} class.
   */
  private static ServerSideEncryptingAmazonS3 makeMockClient(
      final List<S3ObjectSummary> objects
  )
  {
    return new ServerSideEncryptingAmazonS3(null, null)
    {
      @Override
      public ListObjectsV2Result listObjectsV2(final ListObjectsV2Request request)
      {
        // Continuation token is an index in the "objects" list.
        final String continuationToken = request.getContinuationToken();
        final int startIndex = continuationToken == null ? 0 : Integer.parseInt(continuationToken);

        // Find matching objects.
        final List<S3ObjectSummary> summaries = new ArrayList<>();
        int nextIndex = -1;

        for (int i = startIndex; i < objects.size(); i++) {
          final S3ObjectSummary summary = objects.get(i);

          if (summary.getBucketName().equals(request.getBucketName())
              && summary.getKey().startsWith(request.getPrefix())) {

            if (summaries.size() == request.getMaxKeys()) {
              // We reached our max key limit; set nextIndex (which will lead to a result with truncated = true).
              nextIndex = i;
              break;
            }

            // Generate a summary.
            summaries.add(summary);
          }
        }

        // Generate the result.
        final ListObjectsV2Result retVal = new ListObjectsV2Result();
        retVal.setContinuationToken(continuationToken);
        retVal.getObjectSummaries().addAll(summaries);

        if (nextIndex >= 0) {
          retVal.setTruncated(true);
          retVal.setNextContinuationToken(String.valueOf(nextIndex));
        }

        return retVal;
      }
    };
  }

  private static S3ObjectSummary makeObjectSummary(final String bucket, final String key, final long size)
  {
    final S3ObjectSummary summary = new S3ObjectSummary();
    summary.setBucketName(bucket);
    summary.setKey(key);
    summary.setSize(size);
    return summary;
  }
}
