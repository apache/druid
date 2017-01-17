/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.dataformat.smile.SmileGenerator;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.utils.Runnables;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Creates a wrapper firehose that writes from another firehose to disk and then serves nextRow() from disk. Useful for
 * tasks that require multiple passes through the data to prevent multiple remote fetches. Also has support for
 * retrying fetches if the underlying firehose throws an exception while the local cache is being generated.
 */
public class ReplayableFirehoseFactory implements FirehoseFactory<InputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(ReplayableFirehoseFactory.class);
  private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;
  private static final int DEFAULT_MAX_TEMP_FILE_SIZE = 250000000;
  private static final int DEFAULT_READ_FIREHOSE_RETRIES = 3;

  private final FirehoseFactory delegateFactory;
  private final boolean reportParseExceptions;

  // This is *roughly* the max size of the temp files that will be generated, but they may be slightly larger. The
  // reason for the approximation is that we're not forcing flushes after writing to the generator so the number of
  // bytes written to the stream won't be updated until the flush occurs. It's probably more important to optimize for
  // I/O speed rather than maintaining a strict max on the size of the temp file before it's split.
  private final int maxTempFileSize;

  private final int readFirehoseRetries;
  private final ObjectMapper smileMapper;

  private ReplayableFirehose firehose;

  @JsonCreator
  public ReplayableFirehoseFactory(
      @JsonProperty("delegate") FirehoseFactory delegateFactory,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("maxTempFileSize") Integer maxTempFileSize,
      @JsonProperty("readFirehoseRetries") Integer readFirehoseRetries,
      @Smile @JacksonInject ObjectMapper smileMapper
  )
  {
    Preconditions.checkNotNull(delegateFactory, "delegate cannot be null");
    Preconditions.checkArgument(
        !(delegateFactory instanceof ReplayableFirehoseFactory),
        "Refusing to wrap another ReplayableFirehoseFactory"
    );

    this.delegateFactory = delegateFactory;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                 : reportParseExceptions;
    this.maxTempFileSize = maxTempFileSize == null ? DEFAULT_MAX_TEMP_FILE_SIZE : maxTempFileSize;
    this.readFirehoseRetries = readFirehoseRetries == null ? DEFAULT_READ_FIREHOSE_RETRIES : readFirehoseRetries;

    this.smileMapper = smileMapper;

    log.info(this.toString());
  }

  @Override
  public Firehose connect(InputRowParser parser) throws IOException
  {
    if (firehose == null) {
      firehose = new ReplayableFirehose(parser);
    } else {
      log.info("Rewinding and returning existing firehose");
      firehose.rewind();
    }

    return firehose;
  }

  public class ReplayableFirehose implements Firehose
  {
    private final List<File> files = new ArrayList<>();
    private final List<String> dimensions;

    private int fileIndex = 0;
    private JsonFactory jsonFactory;
    private JsonParser jsonParser;
    private Iterator<Row> it;

    public ReplayableFirehose(InputRowParser parser) throws IOException
    {
      jsonFactory = smileMapper.getFactory();

      if (jsonFactory instanceof SmileFactory) {
        jsonFactory = ((SmileFactory) jsonFactory).enable(SmileGenerator.Feature.CHECK_SHARED_STRING_VALUES);
      }

      long counter = 0, totalBytes = 0, unparseable = 0, retryCount = 0;
      Set<String> dimensionScratch = new HashSet<>();

      File tmpDir = Files.createTempDir();
      tmpDir.deleteOnExit();

      long startTime = System.nanoTime();
      boolean isDone = false;
      do {
        deleteTempFiles();
        try (Firehose delegateFirehose = delegateFactory.connect(parser)) {
          while (delegateFirehose.hasMore()) {
            File tmpFile = File.createTempFile("replayable-", null, tmpDir);
            tmpFile.deleteOnExit();

            files.add(tmpFile);
            log.debug("Created file [%s]", tmpFile.getAbsolutePath());

            try (CountingOutputStream cos = new CountingOutputStream(new FileOutputStream(tmpFile));
                 JsonGenerator generator = jsonFactory.createGenerator(cos)) {

              while (delegateFirehose.hasMore() && cos.getCount() < getMaxTempFileSize()) {
                try {
                  InputRow row = delegateFirehose.nextRow();
                  generator.writeObject(row);
                  dimensionScratch.addAll(row.getDimensions());
                  counter++;
                }
                catch (ParseException e) {
                  if (reportParseExceptions) {
                    throw e;
                  }
                  unparseable++;
                }
              }

              totalBytes += cos.getCount();
            }
          }
          isDone = true;
        }
        catch (Exception e) {
          if (++retryCount <= readFirehoseRetries && !(e instanceof ParseException)) {
            log.error(e, "Delegate firehose threw an exception, retrying (%d of %d)", retryCount, readFirehoseRetries);
          } else {
            log.error(e, "Delegate firehose threw an exception, retries exhausted, aborting");
            Throwables.propagate(e);
          }
        }
      } while (!isDone);

      log.info(
          "Finished reading from firehose in [%,dms], [%,d] events parsed, [%,d] bytes written, [%,d] events unparseable",
          (System.nanoTime() - startTime) / 1000000,
          counter,
          totalBytes,
          unparseable
      );

      dimensions = Ordering.natural().immutableSortedCopy(dimensionScratch);

      if (counter == 0) {
        log.warn("Firehose contains no events!");
        deleteTempFiles();
        it = Iterators.emptyIterator();
      } else {
        jsonParser = jsonFactory.createParser(files.get(fileIndex));
        it = jsonParser.readValuesAs(Row.class);
      }
    }

    @Override
    public boolean hasMore()
    {
      if (it.hasNext()) {
        return true;
      }

      try {
        if (jsonParser != null) {
          jsonParser.close();
        }

        if (++fileIndex >= files.size() || files.get(fileIndex).length() == 0) {
          return false;
        }

        jsonParser = jsonFactory.createParser(files.get(fileIndex));
        it = jsonParser.readValuesAs(Row.class);
        return true;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public InputRow nextRow()
    {
      return Rows.toCaseInsensitiveInputRow(it.next(), dimensions);
    }

    @Override
    public Runnable commit()
    {
      return Runnables.getNoopRunnable();
    }

    /**
     * Closes the firehose by closing the input stream and setting an empty iterator. The underlying cache files
     * backing the firehose are retained for when the firehose is "replayed" by calling rewind(). The cache files are
     * deleted by File.deleteOnExit() when the process exits.
     */
    @Override
    public void close() throws IOException
    {
      if (jsonParser != null) {
        jsonParser.close();
      }
      it = Iterators.emptyIterator();
    }

    private void rewind() throws IOException
    {
      close();

      if (!files.isEmpty()) {
        fileIndex = 0;
        jsonParser = jsonFactory.createParser(files.get(fileIndex));
        it = jsonParser.readValuesAs(Row.class);
      }
    }

    private void deleteTempFiles()
    {
      for (File file : files) {
        log.debug("Deleting temp file: %s", file.getAbsolutePath());
        file.delete();
      }

      files.clear();
    }
  }

  @JsonProperty("delegate")
  public FirehoseFactory getDelegateFactory()
  {
    return delegateFactory;
  }

  @JsonProperty("reportParseExceptions")
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @JsonProperty("maxTempFileSize")
  public int getMaxTempFileSize()
  {
    return maxTempFileSize;
  }

  @JsonProperty("readFirehoseRetries")
  public int getReadFirehoseRetries()
  {
    return readFirehoseRetries;
  }

  @Override
  public String toString()
  {
    return "ReplayableFirehoseFactory{" +
           "delegateFactory=" + delegateFactory +
           ", reportParseExceptions=" + reportParseExceptions +
           ", maxTempFileSize=" + maxTempFileSize +
           ", readFirehoseRetries=" + readFirehoseRetries +
           '}';
  }
}
