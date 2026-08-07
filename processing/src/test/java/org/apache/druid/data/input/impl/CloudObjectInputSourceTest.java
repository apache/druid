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

import com.google.common.base.Predicates;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputFilePointer;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.impl.systemfield.SystemField;
import org.apache.druid.data.input.impl.systemfield.SystemFields;
import org.apache.druid.java.util.common.parsers.JSONPathSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
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

  @Test
  public void testGetUris()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assertions.assertEquals(
        URIS,
        inputSource.getUris()
    );
  }

  @Test
  public void testGetPrefixes()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, PREFIXES, null, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assertions.assertEquals(
        PREFIXES,
        inputSource.getPrefixes()
    );
  }

  @Test
  public void testGetObjectGlob()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "**.parquet", SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assertions.assertEquals("**.parquet", inputSource.getObjectGlob());
  }

  @Test
  public void testInequality()
  {
    CloudObjectInputSource inputSource1 = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "**.parquet", SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    CloudObjectInputSource inputSource2 = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, "**.csv", SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    Assertions.assertEquals("**.parquet", inputSource1.getObjectGlob());
    Assertions.assertEquals("**.csv", inputSource2.getObjectGlob());
    Assertions.assertFalse(inputSource2.equals(inputSource1));
  }

  @Test
  public void testWithUrisFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS2, null, null, "**.csv", SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assertions.assertEquals("**.csv", inputSource.getObjectGlob());
    Assertions.assertEquals(URIS, returnedLocationUris);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assertions.assertEquals(URIS.size(), entities.size());
  }

  @Test
  public void testWithUris()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assertions.assertEquals(null, inputSource.getObjectGlob());
    Assertions.assertEquals(URIS, returnedLocationUris);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assertions.assertEquals(URIS.size(), entities.size());
  }

  @Test
  public void testWithObjectsFilter()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS_BEFORE_GLOB, "**.csv", SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    List<URI> returnedLocationUris = returnedLocations.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList());

    Assertions.assertEquals("**.csv", inputSource.getObjectGlob());
    Assertions.assertEquals(URIS, returnedLocationUris);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assertions.assertEquals(OBJECTS.size(), entities.size());
  }

  @Test
  public void testWithObjects()
  {
    CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    Stream<InputSplit<List<CloudObjectLocation>>> splits = inputSource.createSplits(
        new JsonInputFormat(JSONPathSpec.DEFAULT, null, null, null, null),
        new MaxSizeSplitHintSpec(null, 1)
    );

    List<CloudObjectLocation> returnedLocations = splits.map(InputSplit::get).collect(Collectors.toList()).get(0);

    Assertions.assertNull(inputSource.getObjectGlob());
    Assertions.assertEquals(OBJECTS, returnedLocations);

    final List<InputEntity> entities =
        Lists.newArrayList(inputSource.getInputEntities(new JsonInputFormat(null, null, null, null, null)));
    Assertions.assertEquals(OBJECTS.size(), entities.size());
  }

  @Test
  public void test_asFilePointers_withUris()
  {
    final CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS2, null, null, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget(123L));

    final List<InputFilePointer> pointers = inputSource.asFilePointers();

    Assertions.assertEquals(
        URIS2,
        pointers.stream().map(InputFilePointer::uri).collect(Collectors.toList())
    );

    // sizeSupplier defers to the split widget.
    Assertions.assertEquals(123L, pointers.get(0).sizeSupplier().getAsLong());
  }

  @Test
  public void test_asFilePointers_withObjects()
  {
    final CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS_BEFORE_GLOB, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    final List<InputFilePointer> pointers = inputSource.asFilePointers();

    Assertions.assertEquals(
        OBJECTS_BEFORE_GLOB.stream().map(object -> object.toUri(SCHEME)).collect(Collectors.toList()),
        pointers.stream().map(InputFilePointer::uri).collect(Collectors.toList())
    );
  }

  @Test
  public void test_asFilePointers_withObjectGlob()
  {
    final CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, null, OBJECTS_BEFORE_GLOB, "**.csv", SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());

    final List<InputFilePointer> pointers = inputSource.asFilePointers();

    // Only the .csv object survives the glob; the .parquet object is filtered out.
    Assertions.assertEquals(
        URIS,
        pointers.stream().map(InputFilePointer::uri).collect(Collectors.toList())
    );
  }

  @Test
  public void test_asFilePointers_withSystemFieldsReturnsNull()
  {
    final CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null, new SystemFields(EnumSet.of(SystemField.URI)))
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    // System fields cannot be expressed as file pointers.
    Assertions.assertNull(inputSource.asFilePointers());
  }

  @Test
  public void test_asFilePointers_withPrefixesReturnsNull()
  {
    final CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, null, PREFIXES, null, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );

    // Prefixes are not expressed as file pointers.
    Assertions.assertNull(inputSource.asFilePointers());
  }

  @Test
  public void test_asFilePointers_populatorFetchesContent(@TempDir File tempDir) throws IOException
  {
    final byte[] content = "hello,world\n1,2\n".getBytes(StandardCharsets.UTF_8);

    final InputEntity entity = Mockito.mock(InputEntity.class);
    Mockito.when(entity.openRaw()).thenReturn(new ByteArrayInputStream(content));
    Mockito.when(entity.getRetryCondition()).thenReturn(Predicates.alwaysFalse());

    final CloudObjectInputSource inputSource = Mockito.mock(CloudObjectInputSource.class, Mockito.withSettings()
        .useConstructor(SCHEME, URIS, null, null, null, SystemFields.none())
        .defaultAnswer(Mockito.CALLS_REAL_METHODS)
    );
    Mockito.when(inputSource.getSplitWidget()).thenReturn(new MockSplitWidget());
    Mockito.doReturn(entity).when(inputSource).createEntity(ArgumentMatchers.any());

    final List<InputFilePointer> pointers = inputSource.asFilePointers();
    Assertions.assertEquals(1, pointers.size());

    final File dstFile = new File(tempDir, "fetched.csv");
    pointers.get(0).populator().populate(dstFile);

    Assertions.assertArrayEquals(content, Files.readAllBytes(dstFile.toPath()));
  }

  @Test
  public void testGlobSubdirectories()
  {
    PathMatcher m = FileSystems.getDefault().getPathMatcher("glob:**.parquet");
    Assertions.assertTrue(m.matches(Paths.get("db/date=2022-08-01/001.parquet")));
    Assertions.assertTrue(m.matches(Paths.get("db/date=2022-08-01/002.parquet")));

    PathMatcher m2 = FileSystems.getDefault().getPathMatcher("glob:db/date=2022-08-01/*.parquet");
    Assertions.assertTrue(m2.matches(Paths.get("db/date=2022-08-01/001.parquet")));
    Assertions.assertFalse(m2.matches(Paths.get("db/date=2022-08-01/_junk/0/001.parquet")));
  }

  private static class MockSplitWidget implements CloudObjectSplitWidget
  {
    private final long objectSize;

    MockSplitWidget()
    {
      this(0);
    }

    MockSplitWidget(final long objectSize)
    {
      this.objectSize = objectSize;
    }

    @Override
    public Iterator<LocationWithSize> getDescriptorIteratorForPrefixes(List<URI> prefixes)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getObjectSize(CloudObjectLocation descriptor)
    {
      return objectSize;
    }
  }
}
