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

package org.apache.druid.data.input.impl;

import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CloudObjectInputSourceTest
{
  private static final String SCHEME = "s3";

  private static final List<URI> URIS = Collections.singletonList(
      URI.create("s3://foo/bar/file.csv")
  );

  private static final List<URI> URIS2 = Arrays.asList(
      URI.create("s3://foo/bar/file.csv"),
      URI.create("s3://bar/foo/file2.parquet")
  );

  private static final List<URI> PREFIXES = Arrays.asList(
      URI.create("s3://foo/bar/"),
      URI.create("s3://bar/foo/")
  );

  private static final List<CloudObjectLocation> OBJECTS = Collections.singletonList(
      new CloudObjectLocation(URI.create("s3://foo/bar/file.csv"))
  );

  private static final List<CloudObjectLocation> OBJECTS_BEFORE_GLOB = Arrays.asList(
      new CloudObjectLocation(URI.create("s3://foo/bar/file.csv")),
      new CloudObjectLocation(URI.create("s3://bar/foo/file2.parquet"))
  );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testGetUris()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assert.assertEquals(
        URIS,
        inputSource.getUris()
    );
  }

  @Test
  public void testGetPrefixes()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, PREFIXES, null, null)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assert.assertEquals(
        PREFIXES,
        inputSource.getPrefixes()
    );
  }

  @Test
  public void testGetObjectGlob()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "**.parquet")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assert.assertEquals("**.parquet", inputSource.getObjectGlob());
  }

  @Test
  public void testInequality()
  {
    CloudObjectInputSource inputSource1 = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "**.parquet")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    CloudObjectInputSource inputSource2 = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "**.csv")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assert.assertEquals("**.parquet", inputSource1.getObjectGlob());
    Assert.assertEquals("**.csv", inputSource2.getObjectGlob());
    Assert.assertFalse(inputSource2.equals(inputSource1));
  }

  @Test
  public void testWithUrisFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS2, null, null, "**.csv")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assert.assertEquals("**.csv", inputSource.getObjectGlob());
    Assert.assertEquals(URIS, returnedLocationUris);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assert.assertEquals(URIS.size(), entities.size());
  }

  @Test
  public void testWithUris()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assert.assertEquals(null, inputSource.getObjectGlob());
    Assert.assertEquals(URIS, returnedLocationUris);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assert.assertEquals(URIS.size(), entities.size());
  }

  @Test
  public void testWithObjectsFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS_BEFORE_GLOB, "**.csv")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assert.assertEquals("**.csv", inputSource.getObjectGlob());
    Assert.assertEquals(URIS, returnedLocationUris);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assert.assertEquals(OBJECTS.size(), entities.size());
  }

  @Test
  public void testWithObjects()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS, null)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    Assert.assertNull(inputSource.getObjectGlob());
    Assert.assertEquals(OBJECTS, returnedLocations);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assert.assertEquals(OBJECTS.size(), entities.size());
  }

  @Test
  public void testGlobSubdirectories()
  {
    PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:**.parquet");
    Assert.assertTrue(m.matches(Paths.get("db/date=2022-08-01/001.parquet")));
    Assert.assertTrue(m.matches(Paths.get("db/date=2022-08-01/002.parquet")));

    PathMatcher m2 = FileSystems.getDefault().getPathMatcher("glob:db/date=2022-08-01/*.parquet");
    Assert.assertTrue(m2.matches(Paths.get("db/date=2022-08-01/001.parquet")));
    Assert.assertFalse(m2.matches(Paths.get("db/date=2022-08-01/_junk/0/001.parquet")));
  }

  private static class MockSplitWidget implements CloudObjectSplitWidget
  {
    @Override
    public Iterator<LocationWithSize> getDescriptorIteratorForPrefixes(List<URI> prefixes)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getObjectSize(CloudObjectLocation descriptor)
    {
      return 0;
    }
  }
}
