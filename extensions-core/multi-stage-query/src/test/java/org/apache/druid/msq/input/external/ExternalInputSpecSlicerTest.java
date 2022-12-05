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

package org.apache.druid.msq.input.external;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.utils.Streams;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ExternalInputSpecSlicerTest
{
  static final InputFormat INPUT_FORMAT = new JsonInputFormat(null, null, null, null, null);
  static final RowSignature SIGNATURE = RowSignature.builder().add("s", ColumnType.STRING).build();

  private ExternalInputSpecSlicer slicer;

  @Before
  public void setUp()
  {
    slicer = new ExternalInputSpecSlicer();
  }

  @Test
  public void test_canSliceDynamic_splittable()
  {
    Assert.assertTrue(slicer.canSliceDynamic(splittableSpec()));
  }

  @Test
  public void test_canSliceDynamic_unsplittable()
  {
    Assert.assertFalse(slicer.canSliceDynamic(unsplittableSpec()));
  }

  @Test
  public void test_sliceStatic_unsplittable()
  {
    Assert.assertEquals(
        ImmutableList.of(
            unsplittableSlice("foo", "bar", "baz"),
            NilInputSlice.INSTANCE
        ),
        slicer.sliceStatic(unsplittableSpec("foo", "bar", "baz"), 2)
    );
  }

  @Test
  public void test_sliceStatic_splittable()
  {
    Assert.assertEquals(
        ImmutableList.of(
            splittableSlice("foo", "baz"),
            splittableSlice("bar")
        ),
        slicer.sliceStatic(splittableSpec("foo", "bar", "baz"), 2)
    );
  }

  @Test
  public void test_sliceDynamic_unsplittable()
  {
    Assert.assertEquals(
        ImmutableList.of(
            unsplittableSlice("foo", "bar", "baz")
        ),
        slicer.sliceDynamic(unsplittableSpec("foo", "bar", "baz"), 100, 1, 1)
    );
  }

  @Test
  public void test_sliceDynamic_splittable_needOne()
  {
    Assert.assertEquals(
        ImmutableList.of(
            splittableSlice("foo", "bar", "baz")
        ),
        slicer.sliceDynamic(splittableSpec("foo", "bar", "baz"), 100, 5, Long.MAX_VALUE)
    );
  }

  @Test
  public void test_sliceDynamic_splittable_needTwoDueToFiles()
  {
    Assert.assertEquals(
        ImmutableList.of(
            splittableSlice("foo", "baz"),
            splittableSlice("bar")
        ),
        slicer.sliceDynamic(splittableSpec("foo", "bar", "baz"), 100, 2, Long.MAX_VALUE)
    );
  }

  @Test
  public void test_sliceDynamic_splittable_needTwoDueToBytes()
  {
    Assert.assertEquals(
        ImmutableList.of(
            splittableSlice("foo", "baz"),
            splittableSlice("bar")
        ),
        slicer.sliceDynamic(splittableSpec("foo", "bar", "baz"), 100, 5, 7)
    );
  }

  static ExternalInputSpec splittableSpec(final String... strings)
  {
    return new ExternalInputSpec(
        new TestSplittableInputSource(Arrays.asList(strings)),
        INPUT_FORMAT,
        SIGNATURE
    );
  }

  static ExternalInputSpec unsplittableSpec(final String... strings)
  {
    return new ExternalInputSpec(
        new TestUnsplittableInputSource(Arrays.asList(strings)),
        INPUT_FORMAT,
        SIGNATURE
    );
  }

  static ExternalInputSlice splittableSlice(final String... strings)
  {
    return new ExternalInputSlice(
        Stream.of(strings)
              .map(s -> new TestSplittableInputSource(Collections.singletonList(s)))
              .collect(Collectors.toList()),
        INPUT_FORMAT,
        SIGNATURE
    );
  }

  static ExternalInputSlice unsplittableSlice(final String... strings)
  {
    return new ExternalInputSlice(
        Collections.singletonList(new TestUnsplittableInputSource(Arrays.asList(strings))),
        INPUT_FORMAT,
        SIGNATURE
    );
  }

  private static class TestUnsplittableInputSource implements InputSource
  {
    private final List<String> strings;

    public TestUnsplittableInputSource(final List<String> strings)
    {
      this.strings = strings;
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }

    @Override
    public InputSourceReader reader(
        InputRowSchema inputRowSchema,
        @Nullable InputFormat inputFormat,
        File temporaryDirectory
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isSplittable()
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
      TestUnsplittableInputSource that = (TestUnsplittableInputSource) o;
      return Objects.equals(strings, that.strings);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(strings);
    }

    @Override
    public String toString()
    {
      return "TestUnsplittableInputSource{" +
             "strings=" + strings +
             '}';
    }
  }

  private static class TestSplittableInputSource implements SplittableInputSource<List<String>>
  {
    private final List<String> strings;

    public TestSplittableInputSource(final List<String> strings)
    {
      this.strings = strings;
    }

    @Override
    public boolean needsFormat()
    {
      return false;
    }

    @Override
    public InputSourceReader reader(
        InputRowSchema inputRowSchema,
        @Nullable InputFormat inputFormat,
        File temporaryDirectory
    )
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Stream<InputSplit<List<String>>> createSplits(
        InputFormat inputFormat,
        @Nullable SplitHintSpec splitHintSpec
    )
    {
      final Iterator<List<String>> splits = splitHintSpec.split(
          strings.iterator(),
          s -> new InputFileAttribute(s.length())
      );

      return Streams.sequentialStreamFrom(splits).map(InputSplit::new);
    }

    @Override
    public int estimateNumSplits(InputFormat inputFormat, @Nullable SplitHintSpec splitHintSpec)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public InputSource withSplit(InputSplit<List<String>> split)
    {
      return new TestSplittableInputSource(split.get());
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
      TestSplittableInputSource that = (TestSplittableInputSource) o;
      return Objects.equals(strings, that.strings);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(strings);
    }

    @Override
    public String toString()
    {
      return "TestSplittableInputSource{" +
             "strings=" + strings +
             '}';
    }
  }
}
