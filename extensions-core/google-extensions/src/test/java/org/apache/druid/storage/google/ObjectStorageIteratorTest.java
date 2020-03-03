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

import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.storage.google.ObjectStorageIteratorTest.MockStorage.MockObjects.MockList;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ObjectStorageIteratorTest
{
  private static final ImmutableList<StorageObject> TEST_OBJECTS =
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
    final List<StorageObject> expectedObjects = new ArrayList<>();

    // O(N^2) but who cares -- the list is short.
    for (final String uri : expectedUris) {
      final List<StorageObject> matches = TEST_OBJECTS
          .stream()
          .filter(storageObject -> GoogleUtils.objectToUri(storageObject).toString().equals(uri))
          .collect(Collectors.toList());

      expectedObjects.add(Iterables.getOnlyElement(matches));
    }

    final List<StorageObject> actualObjects = ImmutableList.copyOf(
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
  private static GoogleStorage makeMockClient(final List<StorageObject> storageObjects)
  {
    return new GoogleStorage(null)
    {
      @Override
      public Storage.Objects.List list(final String bucket)
      {
        return mockList(bucket, storageObjects);
      }
    };
  }

  @SuppressWarnings("UnnecessaryFullyQualifiedName")
  static class MockStorage extends Storage
  {
    private MockStorage()
    {
      super(
          EasyMock.niceMock(HttpTransport.class),
          EasyMock.niceMock(JsonFactory.class),
          EasyMock.niceMock(HttpRequestInitializer.class)
      );
    }

    private MockList mockList(String bucket, java.util.List<StorageObject> storageObjects)
    {
      return new MockObjects().mockList(bucket, storageObjects);
    }

    class MockObjects extends Storage.Objects
    {
      private MockList mockList(String bucket, java.util.List<StorageObject> storageObjects)
      {
        return new MockList(bucket, storageObjects);
      }

      class MockList extends Objects.List
      {
        private final java.util.List<StorageObject> storageObjects;

        private MockList(String bucket, java.util.List<StorageObject> storageObjects)
        {
          super(bucket);
          this.storageObjects = storageObjects;
        }

        @Override
        public com.google.api.services.storage.model.Objects execute()
        {
          // Continuation token is an index in the "objects" list.
          final String continuationToken = getPageToken();
          final int startIndex = continuationToken == null ? 0 : Integer.parseInt(continuationToken);

          // Find matching objects.
          java.util.List<StorageObject> objects = new ArrayList<>();
          int nextIndex = -1;

          for (int i = startIndex; i < storageObjects.size(); i++) {
            final StorageObject storageObject = storageObjects.get(i);

            if (storageObject.getBucket().equals(getBucket())
                && storageObject.getName().startsWith(getPrefix())) {

              if (objects.size() == getMaxResults()) {
                // We reached our max key limit; set nextIndex (which will lead to a result with truncated = true).
                nextIndex = i;
                break;
              }

              // Generate a summary.
              objects.add(storageObject);
            }
          }

          com.google.api.services.storage.model.Objects retVal = new com.google.api.services.storage.model.Objects();
          retVal.setItems(objects);
          if (nextIndex >= 0) {
            retVal.setNextPageToken(String.valueOf(nextIndex));
          } else {
            retVal.setNextPageToken(null);
          }
          return retVal;
        }
      }
    }
  }

  private static MockList mockList(String bucket, List<StorageObject> storageObjects)
  {
    return new MockStorage().mockList(bucket, storageObjects);
  }

  private static StorageObject makeStorageObject(final String bucket, final String key, final long size)
  {
    final StorageObject summary = new StorageObject();
    summary.setBucket(bucket);
    summary.setName(key);
    summary.setSize(BigInteger.valueOf(size));
    return summary;
  }
}
