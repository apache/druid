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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
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
  private static final ImmutableList<OSSObjectSummary> TEST_OBJECTS =
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
        ImmutableList.of("oss://b/foo/baz"),
        ImmutableList.of("oss://b/foo/baz"),
        5
    );
  }

  @Test
  public void testMultiObjectOneKeyAtATime()
  {
    test(
        ImmutableList.of(
            "oss://b/foo/bar1",
            "oss://b/foo/bar2",
            "oss://b/foo/bar3",
            "oss://b/foo/bar4",
            "oss://b/foo/baz"
        ),
        ImmutableList.of("oss://b/foo/"),
        1
    );
  }

  @Test
  public void testMultiObjectTwoKeysAtATime()
  {
    test(
        ImmutableList.of(
            "oss://b/foo/bar1",
            "oss://b/foo/bar2",
            "oss://b/foo/bar3",
            "oss://b/foo/bar4",
            "oss://b/foo/baz"
        ),
        ImmutableList.of("oss://b/foo/"),
        2
    );
  }

  @Test
  public void testMultiObjectTenKeysAtATime()
  {
    test(
        ImmutableList.of(
            "oss://b/foo/bar1",
            "oss://b/foo/bar2",
            "oss://b/foo/bar3",
            "oss://b/foo/bar4",
            "oss://b/foo/baz"
        ),
        ImmutableList.of("oss://b/foo/"),
        10
    );
  }

  @Test
  public void testPrefixInMiddleOfKey()
  {
    test(
        ImmutableList.of("oss://b/foo/bar1", "oss://b/foo/bar2", "oss://b/foo/bar3", "oss://b/foo/bar4"),
        ImmutableList.of("oss://b/foo/bar"),
        10
    );
  }

  @Test
  public void testNoPath()
  {
    test(
        ImmutableList.of(
            "oss://b/foo",
            "oss://b/foo/bar1",
            "oss://b/foo/bar2",
            "oss://b/foo/bar3",
            "oss://b/foo/bar4",
            "oss://b/foo/baz"
        ),
        ImmutableList.of("oss://b"),
        10
    );
  }

  @Test
  public void testSlashPath()
  {
    test(
        ImmutableList.of(
            "oss://b/foo",
            "oss://b/foo/bar1",
            "oss://b/foo/bar2",
            "oss://b/foo/bar3",
            "oss://b/foo/bar4",
            "oss://b/foo/baz"
        ),
        ImmutableList.of("oss://b/"),
        10
    );
  }

  @Test
  public void testDifferentBucket()
  {
    test(
        ImmutableList.of(),
        ImmutableList.of("oss://bx/foo/"),
        10
    );
  }

  @Test
  public void testWithMultiplePrefixesReturningAllNonEmptyObjectsStartingWithOneOfPrefixes()
  {
    test(
        ImmutableList.of(
            "oss://b/foo/bar1",
            "oss://b/foo/bar2",
            "oss://b/foo/bar3",
            "oss://b/foo/bar4",
            "oss://b/foo/baz"
        ),
        ImmutableList.of("oss://b/foo/bar", "oss://b/foo/baz"),
        10
    );
  }

  private static void test(
      final List<String> expectedUris,
      final List<String> prefixes,
      final int maxListingLength
  )
  {
    final List<OSSObjectSummary> expectedObjects = new ArrayList<>();

    // O(N^2) but who cares -- the list is short.
    for (final String uri : expectedUris) {
      final List<OSSObjectSummary> matches = TEST_OBJECTS.stream()
                                                         .filter(
                                                             summary ->
                                                                 OssUtils.summaryToUri(summary).toString().equals(uri)
                                                         )
                                                         .collect(Collectors.toList());

      expectedObjects.add(Iterables.getOnlyElement(matches));
    }

    final List<OSSObjectSummary> actualObjects = ImmutableList.copyOf(
        OssUtils.objectSummaryIterator(
            makeMockClient(TEST_OBJECTS),
            prefixes.stream().map(URI::create).collect(Collectors.toList()),
            maxListingLength
        )
    );

    Assert.assertEquals(
        prefixes.toString(),
        expectedObjects.stream().map(OssUtils::summaryToUri).collect(Collectors.toList()),
        actualObjects.stream().map(OssUtils::summaryToUri).collect(Collectors.toList())
    );
  }

  /**
   * Makes a mock S3 client that handles enough of "listObjectsV2" to test the functionality of the
   * {@link ObjectSummaryIterator} class.
   */
  private static OSS makeMockClient(
      final List<OSSObjectSummary> objects
  )
  {
    return new OSSClient("endpoint", "accessKey", "keySecret")
    {
      @Override
      public ObjectListing listObjects(final ListObjectsRequest request)
      {
        // Continuation token is an index in the "objects" list.q
        final String continuationToken = request.getMarker();
        final int startIndex = continuationToken == null ? 0 : Integer.parseInt(continuationToken);

        // Find matching objects.
        final List<OSSObjectSummary> summaries = new ArrayList<>();
        int nextIndex = -1;

        for (int i = startIndex; i < objects.size(); i++) {
          final OSSObjectSummary summary = objects.get(i);

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
        final ObjectListing retVal = new ObjectListing();
        retVal.getObjectSummaries().addAll(summaries);

        if (nextIndex >= 0) {
          retVal.setTruncated(true);
          retVal.setNextMarker(String.valueOf(nextIndex));
        }

        return retVal;
      }
    };
  }

  private static OSSObjectSummary makeObjectSummary(final String bucket, final String key, final long size)
  {
    final OSSObjectSummary summary = new OSSObjectSummary();
    summary.setBucketName(bucket);
    summary.setKey(key);
    summary.setSize(size);
    return summary;
  }
}
