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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.data.input.InputStats;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.msq.counters.ChannelCounters;
import org.apache.druid.msq.counters.WarningCounters;
import org.apache.druid.msq.indexing.error.CannotParseExternalDataFault;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.incremental.NoopRowIngestionMeters;
import org.apache.druid.utils.CloseableUtils;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Segment representing the rows read from an external source. This is currently returned when using EXTERN with MSQ
 */
public class ExternalSegment extends RowBasedSegment<InputRow>
{
  private final RowSignature signature;

  /**
   * @param inputSource       {@link InputSource} that the segment is a representation of
   * @param reader            reader to read the external input source
   * @param warningCounters   warning counters tracking the warnings generated while reading the external source
   * @param warningPublisher  publisher to report the warnings generated
   * @param channelCounters   channel counters, will be used for {@link ChannelCounters#incrementBytes(long)}
   * @param signature         signature of the external source
   */
  public ExternalSegment(
      final InputSource inputSource,
      final InputSourceReader reader,
      final WarningCounters warningCounters,
      final Consumer<Throwable> warningPublisher,
      final ChannelCounters channelCounters,
      final RowSignature signature
  )
  {
    super(
        new BaseSequence<>(
            new BaseSequence.IteratorMaker<InputRow, CloseableIterator<InputRow>>()
            {
              @Override
              public CloseableIterator<InputRow> make()
              {
                try {
                  CloseableIterator<InputRow> baseIterator = reader.read(makeInputStats(inputSource, channelCounters));
                  return new CloseableIterator<>()
                  {
                    private InputRow next = null;

                    @Override
                    public void close() throws IOException
                    {
                      baseIterator.close();
                    }

                    @Override
                    public boolean hasNext()
                    {
                      while (true) {
                        try {
                          while (next == null && baseIterator.hasNext()) {
                            next = baseIterator.next();
                          }
                          break;
                        }
                        catch (ParseException e) {
                          warningCounters.incrementWarningCount(CannotParseExternalDataFault.CODE);
                          warningPublisher.accept(e);
                        }
                      }
                      return next != null;
                    }

                    @Override
                    public InputRow next()
                    {
                      if (!hasNext()) {
                        throw new NoSuchElementException();
                      }
                      final InputRow row = next;
                      next = null;
                      return row;
                    }
                  };
                }
                catch (IOException e) {
                  throw new RuntimeException(e);
                }
              }

              @Override
              public void cleanup(CloseableIterator<InputRow> iterFromMake)
              {
                CloseableUtils.closeAndWrapExceptions(iterFromMake);
              }
            }
        ),
        reader.rowAdapter(),
        signature
    );
    this.signature = signature;
  }

  /**
   * Returns the signature of the external input source
   */
  public RowSignature signature()
  {
    return signature;
  }

  /**
   * Create {@link InputStats} that calls {@link ChannelCounters#incrementBytes(long)} as data is read from files.
   */
  private static InputStats makeInputStats(final InputSource inputSource, final ChannelCounters channelCounters)
  {
    if (ExternalInputSliceReader.isFileBasedInputSource(inputSource)) {
      return new InputStats()
      {
        private final AtomicLong processedBytes = new AtomicLong();

        @Override
        public void incrementProcessedBytes(final long n)
        {
          processedBytes.addAndGet(n);
          channelCounters.incrementBytes(n);
        }

        @Override
        public long getProcessedBytes()
        {
          return processedBytes.get();
        }
      };
    } else {
      return new NoopRowIngestionMeters();
    }
  }
}
