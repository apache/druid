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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.AbstractInputSource;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Pair;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CombiningInputSourceTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(new SimpleModule("test-module").registerSubtypes(TestFileInputSource.class, TestUriInputSource.class));
    final TestFileInputSource fileSource = new TestFileInputSource(ImmutableList.of(new File("myFile").getAbsoluteFile()));
    final TestUriInputSource uriInputSource = new TestUriInputSource(
        ImmutableList.of(URI.create("http://test.com/http-test")));
    final CombiningInputSource combiningInputSource = new CombiningInputSource(ImmutableList.of(
        fileSource,
        uriInputSource
    ));
    final byte[] json = mapper.writeValueAsBytes(combiningInputSource);
    final CombiningInputSource fromJson = (CombiningInputSource) mapper.readValue(json, InputSource.class);
    Assert.assertEquals(combiningInputSource, fromJson);
  }

  @Test
  public void testEstimateNumSplits()
  {
    final File file = EasyMock.niceMock(File.class);
    EasyMock.expect(file.length()).andReturn(5L).anyTimes();
    EasyMock.replay(file);
    final TestFileInputSource fileSource = new TestFileInputSource(generateFiles(3));
    final TestUriInputSource uriInputSource = new TestUriInputSource(
        ImmutableList.of(
            URI.create("http://test.com/http-test1"),
            URI.create("http://test.com/http-test2"),
            URI.create("http://test.com/http-test3")
        )
    );
    final CombiningInputSource combiningInputSource = new CombiningInputSource(ImmutableList.of(
        fileSource,
        uriInputSource
    ));
    Assert.assertEquals(combiningInputSource.estimateNumSplits(
        new NoopInputFormat(),
        new MaxSizeSplitHintSpec(
            new HumanReadableBytes(5L),
            null
        )
    ), 6);
  }


  @Test
  public void testCreateSplits()
  {
    final File file = EasyMock.niceMock(File.class);
    EasyMock.expect(file.length()).andReturn(30L).anyTimes();
    EasyMock.replay(file);
    final TestFileInputSource fileSource = new TestFileInputSource(generateFiles(3));
    final TestUriInputSource uriInputSource = new TestUriInputSource(
        ImmutableList.of(
            URI.create("http://test.com/http-test3"),
            URI.create("http://test.com/http-test4"),
            URI.create("http://test.com/http-test5")
        )
    );
    final CombiningInputSource combiningInputSource = new CombiningInputSource(ImmutableList.of(
        fileSource,
        uriInputSource
    ));
    List<InputSplit> combinedInputSplits = combiningInputSource.createSplits(
        new NoopInputFormat(),
        new MaxSizeSplitHintSpec(
            new HumanReadableBytes(5L),
            null
        )
    ).collect(Collectors.toList());
    Assert.assertEquals(6, combinedInputSplits.size());
    for (int i = 0; i < 3; i++) {
      Pair<SplittableInputSource, InputSplit> splitPair = (Pair) combinedInputSplits.get(i).get();
      InputSplit<File> fileSplits = splitPair.rhs;
      Assert.assertTrue(splitPair.lhs instanceof TestFileInputSource);
      Assert.assertEquals(5, fileSplits.get().length());
    }
    for (int i = 3; i < combinedInputSplits.size(); i++) {
      Pair<SplittableInputSource, InputSplit> splitPair = (Pair) combinedInputSplits.get(i).get();
      InputSplit<URI> fileSplits = splitPair.rhs;
      Assert.assertTrue(splitPair.lhs instanceof TestUriInputSource);
      Assert.assertEquals(URI.create("http://test.com/http-test" + i), fileSplits.get());
    }
  }

  @Test
  public void testWithSplits()
  {
    final TestUriInputSource uriInputSource = new TestUriInputSource(
        ImmutableList.of(
            URI.create("http://test.com/http-test1"))
    );
    final CombiningInputSource combiningInputSource = new CombiningInputSource(ImmutableList.of(
        uriInputSource
    ));
    InputSplit<URI> testUriSplit = new InputSplit<>(URI.create("http://test.com/http-test1"));
    TestUriInputSource urlInputSourceWithSplit = (TestUriInputSource) combiningInputSource.withSplit(new InputSplit(Pair.of(
        uriInputSource,
        testUriSplit)));
    Assert.assertEquals(uriInputSource, urlInputSourceWithSplit);

  }

  @Test
  public void testNeedsFormat()
  {
    final TestUriInputSource uriInputSource = new TestUriInputSource(
        ImmutableList.of(
            URI.create("http://test.com/http-test1")
        )
    );
    final TestFileInputSource fileSource = new TestFileInputSource(generateFiles(3));

    final CombiningInputSource combiningInputSource = new CombiningInputSource(ImmutableList.of(
        uriInputSource,
        fileSource
    ));
    Assert.assertTrue(combiningInputSource.needsFormat());

  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(CombiningInputSource.class)
                  .withNonnullFields("delegates")
                  .usingGetClass()
                  .verify();
  }

  private static List<File> generateFiles(int numFiles)
  {
    final List<File> files = new ArrayList<>();
    for (int i = 0; i < numFiles; i++) {
      final File file = EasyMock.niceMock(File.class);
      EasyMock.expect(file.length()).andReturn(5L).anyTimes();
      EasyMock.replay(file);
      files.add(file);
    }
    return files;
  }

  private static class TestFileInputSource extends AbstractInputSource implements SplittableInputSource<File>
  {
    private final List<File> files;

    @JsonCreator
    private TestFileInputSource(@JsonProperty("files") List<File> fileList)
    {
      files = fileList;
    }

    @JsonProperty
    public List<File> getFiles()
    {
      return files;
    }

    @Override
    public Stream<InputSplit<File>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return files.stream().map(InputSplit::new);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return files.size();
    }

    @Override
    public SplittableInputSource<File> withSplit(InputSplit<File> split)
    {
      return new TestFileInputSource(ImmutableList.of(split.get()));
    }

    @Override
    public boolean needsFormat()
    {
      return true;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestFileInputSource that = (TestFileInputSource) o;
      return Objects.equals(files, that.files);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(files);
    }
  }

  private static class TestUriInputSource extends AbstractInputSource implements SplittableInputSource<URI>
  {
    private final List<URI> uris;

    @JsonCreator
    private TestUriInputSource(@JsonProperty("uris") List<URI> uriList)
    {
      uris = uriList;
    }

    @JsonProperty
    public List<URI> getUris()
    {
      return uris;
    }

    @Override
    public Stream<InputSplit<URI>> createSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return uris.stream().map(InputSplit::new);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      return uris.size();
    }

    @Override
    public SplittableInputSource<URI> withSplit(InputSplit<URI> split)
    {
      return new TestUriInputSource(ImmutableList.of(split.get()));
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TestUriInputSource that = (TestUriInputSource) o;
      return Objects.equals(uris, that.uris);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(uris);
    }
  }
}
