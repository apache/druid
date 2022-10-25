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

import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
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

  private static final List<CloudObjectLocation> OBJECTS_BEFORE_FILTER = Arrays.asList(
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
  public void testGetFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "*.parquet")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assert.assertEquals("*.parquet", inputSource.getFilter());
  }

  @Test
  public void testInequality()
  {
    CloudObjectInputSource inputSource1 = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "*.parquet")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    CloudObjectInputSource inputSource2 = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "*.csv")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assert.assertEquals("*.parquet", inputSource1.getFilter());
    Assert.assertEquals("*.csv", inputSource2.getFilter());
    Assert.assertFalse(inputSource2.equals(inputSource1));
  }

  @Test
  public void testWithUrisFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS2, null, null, "*.csv")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assert.assertEquals("*.csv", inputSource.getFilter());
    Assert.assertEquals(URIS, returnedLocationUris);
  }

  @Test
  public void testWithUris()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assert.assertEquals(null, inputSource.getFilter());
    Assert.assertEquals(URIS, returnedLocationUris);
  }

  @Test
  public void testWithObjectsFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS_BEFORE_FILTER, "*.csv")
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assert.assertEquals("*.csv", inputSource.getFilter());
    Assert.assertEquals(URIS, returnedLocationUris);
  }

  @Test
  public void testWithObjects()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS, null)
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    Assert.assertEquals(null, inputSource.getFilter());
    Assert.assertEquals(OBJECTS, returnedLocations);
  }
}
