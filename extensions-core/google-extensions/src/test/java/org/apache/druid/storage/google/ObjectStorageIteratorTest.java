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

//import com.google.api.client.http.HttpRequestInitializer;
//import com.google.api.client.http.HttpTransport;
//import com.google.api.client.json.JsonFactory;
//import com.google.api.services.storage.Storage;
//import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectStorageIteratorTest
{
  private static final ImmutableList<GoogleStorage.GoogleStorageObjectMetadata> TEST_OBJECTS =
      ImmutableList.of(
          makeStorageObject("b", "foo", 10L),
          makeStorageObject("b", "foo/", 0L), // directory
          makeStorageObject("b", "foo/bar1", 10L),
          makeStorageObject("b", "foo/bar2", 10L),
          makeStorageObject("b", "foo/bar3", 10L),
          makeStorageObject("b", "foo/bar4", 10L),
          makeStorageObject("b", "foo/bar5", 0L), // empty object
          makeStorageObject("b", "foo/baz", 10L),
          makeStorageObject("bucketnotmine", "a/different/bucket", 10L),
          makeStorageObject("b", "foo/bar/", 0L) // another directory at the end of list
      );

  @Test
  public void testSingleObject()
  {
     test(
        ImmutableList.of("gs://b/foo/baz"),
        ImmutableList.of("gs://b/foo/baz"),
        5
    );
  }

  @Test
  public void testMultiObjectOneKeyAtATime()
  {
    test(
        ImmutableList.of("gs://b/foo/bar1", "gs://b/foo/bar2", "gs://b/foo/bar3", "gs://b/foo/bar4", "gs://b/foo/baz"),
        ImmutableList.of("gs://b/foo/"),
        1
    );
  }

  @Test
  public void testMultiObjectTwoKeysAtATime()
  {
    test(
        ImmutableList.of("gs://b/foo/bar1", "gs://b/foo/bar2", "gs://b/foo/bar3", "gs://b/foo/bar4", "gs://b/foo/baz"),
        ImmutableList.of("gs://b/foo/"),
        2
    );
  }

  @Test
  public void testMultiObjectTenKeysAtATime()
  {
    test(
        ImmutableList.of("gs://b/foo/bar1", "gs://b/foo/bar2", "gs://b/foo/bar3", "gs://b/foo/bar4", "gs://b/foo/baz"),
        ImmutableList.of("gs://b/foo/"),
        10
    );
  }

  @Test
  public void testPrefixInMiddleOfKey()
  {
    test(
        ImmutableList.of("gs://b/foo/bar1", "gs://b/foo/bar2", "gs://b/foo/bar3", "gs://b/foo/bar4"),
        ImmutableList.of("gs://b/foo/bar"),
        10
    );
  }

  @Test
  public void testNoPath()
  {
    test(
        ImmutableList.of(
            "gs://b/foo",
            "gs://b/foo/bar1",
            "gs://b/foo/bar2",
            "gs://b/foo/bar3",
            "gs://b/foo/bar4",
            "gs://b/foo/baz"
        ),
        ImmutableList.of("gs://b"),
        10
    );
  }

  @Test
  public void testSlashPath()
  {
    test(
        ImmutableList.of(
            "gs://b/foo",
            "gs://b/foo/bar1",
            "gs://b/foo/bar2",
            "gs://b/foo/bar3",
            "gs://b/foo/bar4",
            "gs://b/foo/baz"
        ),
        ImmutableList.of("gs://b/"),
        10
    );
  }

  @Test
  public void testDifferentBucket()
  {
    test(
        ImmutableList.of(),
        ImmutableList.of("gs://bx/foo/"),
        10
    );
  }

  @Test
  public void testWithMultiplePrefixesReturningAllNonEmptyObjectsStartingWithOneOfPrefixes()
  {
    test(
        ImmutableList.of("gs://b/foo/bar1", "gs://b/foo/bar2", "gs://b/foo/bar3", "gs://b/foo/bar4", "gs://b/foo/baz"),
        ImmutableList.of("gs://b/foo/bar", "gs://b/foo/baz"),
        10
    );
  }

  private static void test(
      final List<String> expectedUris,
      final List<String> prefixes,
      final int maxListingLength
  )
  {
    final List<GoogleStorage.GoogleStorageObjectMetadata> expectedObjects = new ArrayList<>();

    // O(N^2) but who cares -- the list is short.
    for (final String uri : expectedUris) {
      final List<GoogleStorage.GoogleStorageObjectMetadata> matches = TEST_OBJECTS
          .stream()
          .filter(storageObject -> GoogleUtils.objectToUri(storageObject).toString().equals(uri))
          .collect(Collectors.toList());

      expectedObjects.add(Iterables.getOnlyElement(matches));
    }

    final List<GoogleStorage.GoogleStorageObjectMetadata> actualObjects = ImmutableList.copyOf(
        GoogleUtils.lazyFetchingStorageObjectsIterator(
            makeMockClient(TEST_OBJECTS),
            prefixes.stream().map(URI::create).iterator(),
            maxListingLength
        )
    );

    Assert.assertEquals(
        prefixes.toString(),
        expectedObjects.stream().map(GoogleUtils::objectToUri).collect(Collectors.toList()),
        actualObjects.stream().map(GoogleUtils::objectToUri).collect(Collectors.toList())
    );
  }

  /**
   * Makes a mock Google Storage client that handles enough of "List" to test the functionality of the
   * {@link ObjectStorageIterator} class.
   */
  static GoogleStorage makeMockClient(final List<GoogleStorage.GoogleStorageObjectMetadata> storageObjects)
  {
    return new GoogleStorage(null)
    {
      @Override
      public GoogleStorageObjectPage list(
          final String bucket, final String prefix, final Long pageSize, final String pageToken
      )
      {
        return new GoogleStorageObjectPage(storageObjects, null);
      }
    };
  }
  static GoogleStorage.GoogleStorageObjectMetadata makeStorageObject(final String bucket, final String key, final long size)
  {
    return new GoogleStorage.GoogleStorageObjectMetadata(bucket, key, size, null);
  }
}
