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

import com.google.common.collect.Iterators;
import org.apache.druid.data.input.FilePerSplitHintSpec;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.SlicerUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Slices {@link ExternalInputSpec} into {@link ExternalInputSlice} or {@link NilInputSlice}.
 */
public class ExternalInputSpecSlicer implements InputSpecSlicer
{
  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return ((ExternalInputSpec) inputSpec).getInputSource().isSplittable();
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    final ExternalInputSpec externalInputSpec = (ExternalInputSpec) inputSpec;

    if (externalInputSpec.getInputSource().isSplittable()) {
      return sliceSplittableInputSource(
          externalInputSpec,
          new StaticSplitHintSpec(maxNumSlices),
          maxNumSlices
      );
    } else {
      return sliceUnsplittableInputSource(externalInputSpec);
    }
  }

  @Override
  public List<InputSlice> sliceDynamic(
      final InputSpec inputSpec,
      final int maxNumSlices,
      final int maxFilesPerSlice,
      final long maxBytesPerSlice
  )
  {
    final ExternalInputSpec externalInputSpec = (ExternalInputSpec) inputSpec;

    if (externalInputSpec.getInputSource().isSplittable()) {
      return sliceSplittableInputSource(
          externalInputSpec,
          new DynamicSplitHintSpec(maxNumSlices, maxFilesPerSlice, maxBytesPerSlice),
          maxNumSlices
      );
    } else {
      return sliceUnsplittableInputSource(externalInputSpec);
    }
  }

  /**
   * "Slice" an unsplittable input source into a single slice.
   */
  private static List<InputSlice> sliceUnsplittableInputSource(final ExternalInputSpec inputSpec)
  {
    return Collections.singletonList(
        new ExternalInputSlice(
            Collections.singletonList(inputSpec.getInputSource()),
            inputSpec.getInputFormat(),
            inputSpec.getSignature()
        )
    );
  }

  /**
   * Slice a {@link SplittableInputSource} using a {@link SplitHintSpec}.
   */
  private static List<InputSlice> sliceSplittableInputSource(
      final ExternalInputSpec inputSpec,
      final SplitHintSpec splitHintSpec,
      final int maxNumSlices
  )
  {
    final SplittableInputSource<Object> splittableInputSource =
        (SplittableInputSource<Object>) inputSpec.getInputSource();

    try {
      final List<InputSplit<Object>> splitList =
          splittableInputSource.createSplits(inputSpec.getInputFormat(), splitHintSpec).collect(Collectors.toList());
      final List<InputSlice> assignments = new ArrayList<>();

      if (splitList.size() <= maxNumSlices) {
        for (final InputSplit<Object> split : splitList) {
          assignments.add(splitsToSlice(inputSpec, Collections.singletonList(split)));
        }
      } else {
        // In some cases (for example, HttpInputSource) "createSplits" ignores our splitHintSpec. If this happens,
        // the number of splits may be larger than maxNumSlices. Remix the splits ourselves.
        final List<List<InputSplit<Object>>> splitsList =
            SlicerUtils.makeSlicesStatic(splitList.iterator(), maxNumSlices);

        for (List<InputSplit<Object>> splits : splitsList) {
          //noinspection rawtypes, unchecked
          assignments.add(splitsToSlice(inputSpec, (List) splits));
        }
      }

      return assignments;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Convert {@link InputSplit} (from {@link SplittableInputSource#createSplits}) into an {@link InputSlice}.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static InputSlice splitsToSlice(
      final ExternalInputSpec spec,
      final List<InputSplit<?>> splits
  )
  {
    try {
      final SplittableInputSource<?> splittableInputSource = (SplittableInputSource) spec.getInputSource();
      final List<InputSource> subSources = new ArrayList<>();

      for (final InputSplit<?> split : splits) {
        // Use FilePerSplitHintSpec to create an InputSource per file. This allows to us track progress at
        // the level of reading individual files.
        ((SplittableInputSource<?>) splittableInputSource.withSplit((InputSplit) split))
            .createSplits(spec.getInputFormat(), FilePerSplitHintSpec.INSTANCE)
            .map(subSplit -> splittableInputSource.withSplit((InputSplit) subSplit))
            .forEach(s -> ((List) subSources).add(s));
      }

      if (subSources.isEmpty()) {
        return NilInputSlice.INSTANCE;
      } else {
        return new ExternalInputSlice(subSources, spec.getInputFormat(), spec.getSignature());
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Split hint spec used by {@link #sliceStatic(InputSpec, int)}.
   */
  static class StaticSplitHintSpec implements SplitHintSpec
  {
    private final int maxNumSlices;

    public StaticSplitHintSpec(final int maxNumSlices)
    {
      this.maxNumSlices = maxNumSlices;
    }

    @Override
    public <T> Iterator<List<T>> split(
        final Iterator<T> inputIterator,
        final Function<T, InputFileAttribute> inputAttributeExtractor
    )
    {
      return Iterators.filter(
          SlicerUtils.makeSlicesStatic(
              inputIterator,
              item -> inputAttributeExtractor.apply(item).getSize(),
              maxNumSlices
          ).iterator(),
          xs -> !xs.isEmpty()
      );
    }
  }

  /**
   * Split hint spec used by {@link #sliceDynamic(InputSpec, int, int, long)}.
   */
  static class DynamicSplitHintSpec implements SplitHintSpec
  {
    private final int maxNumSlices;
    private final int maxFilesPerSlice;
    private final long maxBytesPerSlice;

    public DynamicSplitHintSpec(final int maxNumSlices, final int maxFilesPerSlice, final long maxBytesPerSlice)
    {
      this.maxNumSlices = maxNumSlices;
      this.maxFilesPerSlice = maxFilesPerSlice;
      this.maxBytesPerSlice = maxBytesPerSlice;
    }

    @Override
    public <T> Iterator<List<T>> split(
        final Iterator<T> inputIterator,
        final Function<T, InputFileAttribute> inputAttributeExtractor
    )
    {
      return Iterators.filter(
          SlicerUtils.makeSlicesDynamic(
              inputIterator,
              item -> inputAttributeExtractor.apply(item).getWeightedSize(),
              maxNumSlices,
              maxFilesPerSlice,
              maxBytesPerSlice
          ).iterator(),
          xs -> !xs.isEmpty()
      );
    }
  }
}
