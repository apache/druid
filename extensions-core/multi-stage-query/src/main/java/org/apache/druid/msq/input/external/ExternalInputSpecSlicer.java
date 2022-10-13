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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.druid.data.input.InputFileAttribute;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.MaxSizeSplitHintSpec;
import org.apache.druid.data.input.SplitHintSpec;
import org.apache.druid.data.input.impl.SplittableInputSource;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.input.InputSlice;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.NilInputSlice;
import org.apache.druid.msq.input.SlicerUtils;
import org.apache.druid.segment.column.RowSignature;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
    final InputSource inputSource = externalInputSpec.getInputSource();
    final InputFormat inputFormat = externalInputSpec.getInputFormat();
    final RowSignature signature = externalInputSpec.getSignature();

    // Worker number -> input source for that worker.
    final List<List<InputSource>> workerInputSourcess;

    // Figure out input splits for each worker.
    if (inputSource.isSplittable()) {
      //noinspection unchecked
      final SplittableInputSource<Object> splittableInputSource = (SplittableInputSource<Object>) inputSource;

      try {
        workerInputSourcess = SlicerUtils.makeSlices(
            splittableInputSource.createSplits(inputFormat, FilePerSplitHintSpec.INSTANCE)
                                 .map(splittableInputSource::withSplit)
                                 .iterator(),
            maxNumSlices
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      workerInputSourcess = Collections.singletonList(Collections.singletonList(inputSource));
    }

    // Sanity check. It is a bug in this method if this exception is ever thrown.
    if (workerInputSourcess.size() > maxNumSlices) {
      throw new ISE("Generated too many slices [%d > %d]", workerInputSourcess.size(), maxNumSlices);
    }

    return IntStream.range(0, maxNumSlices)
                    .mapToObj(
                        workerNumber -> {
                          final List<InputSource> workerInputSources;

                          if (workerNumber < workerInputSourcess.size()) {
                            workerInputSources = workerInputSourcess.get(workerNumber);
                          } else {
                            workerInputSources = Collections.emptyList();
                          }

                          if (workerInputSources.isEmpty()) {
                            return NilInputSlice.INSTANCE;
                          } else {
                            return new ExternalInputSlice(workerInputSources, inputFormat, signature);
                          }
                        }
                    )
                    .collect(Collectors.toList());
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    final ExternalInputSpec externalInputSpec = (ExternalInputSpec) inputSpec;

    if (!externalInputSpec.getInputSource().isSplittable()) {
      return sliceStatic(inputSpec, 1);
    }

    final SplittableInputSource<?> inputSource = (SplittableInputSource<?>) externalInputSpec.getInputSource();
    final MaxSizeSplitHintSpec maxSizeSplitHintSpec = new MaxSizeSplitHintSpec(
        new HumanReadableBytes(maxBytesPerSlice),
        maxFilesPerSlice
    );

    final long numSlices;

    try {
      numSlices = inputSource.createSplits(externalInputSpec.getInputFormat(), maxSizeSplitHintSpec).count();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    return sliceStatic(inputSpec, (int) Math.min(numSlices, maxNumSlices));
  }

  @VisibleForTesting
  static class FilePerSplitHintSpec implements SplitHintSpec
  {
    static FilePerSplitHintSpec INSTANCE = new FilePerSplitHintSpec();

    private FilePerSplitHintSpec()
    {
      // Singleton.
    }

    @Override
    public <T> Iterator<List<T>> split(
        final Iterator<T> inputIterator,
        final Function<T, InputFileAttribute> inputAttributeExtractor
    )
    {
      return Iterators.transform(inputIterator, Collections::singletonList);
    }
  }
}
