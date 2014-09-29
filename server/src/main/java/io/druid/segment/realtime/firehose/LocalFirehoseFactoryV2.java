/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;

import io.druid.data.input.Committer;
import io.druid.data.input.FirehoseFactoryV2;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 */
public class LocalFirehoseFactoryV2 implements FirehoseFactoryV2<StringInputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(LocalFirehoseFactory.class);

  private final File baseDir;
  private final String filter;
  private final StringInputRowParser parser;

  @JsonCreator
  public LocalFirehoseFactoryV2(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter,
      // Backwards compatible
      @JsonProperty("parser") StringInputRowParser parser
  )
  {
    this.baseDir = baseDir;
    this.filter = filter;
    this.parser = parser;
  }

  @JsonProperty
  public File getBaseDir()
  {
    return baseDir;
  }

  @JsonProperty
  public String getFilter()
  {
    return filter;
  }

  @JsonProperty
  public StringInputRowParser getParser()
  {
    return parser;
  }

  @Override
  public FirehoseV2 connect(StringInputRowParser firehoseParser, Object metadata) throws IOException, ParseException
  {
    log.info("Searching for all [%s] in and beneath [%s]", filter, baseDir.getAbsoluteFile());

    Collection<File> foundFiles = FileUtils.listFiles(
        baseDir.getAbsoluteFile(),
        new WildcardFileFilter(filter),
        TrueFileFilter.INSTANCE
    );

    if (foundFiles == null || foundFiles.isEmpty()) {
      throw new ISE("Found no files to ingest! Check your schema.");
    }
    log.info ("Found files: " + foundFiles);

    final LinkedList<File> files = Lists.newLinkedList(
        foundFiles
    );
    return new FileIteratingFirehoseV2(new Iterator<LineIterator>()
    {
      @Override
      public boolean hasNext()
      {
        return !files.isEmpty();
      }

      @Override
      public LineIterator next()
      {
        try {
          return FileUtils.lineIterator(files.poll());
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public void remove()
      {
        throw new UnsupportedOperationException();
      }
    }, firehoseParser);

    
   
  }
  class FileIteratingFirehoseV2 implements FirehoseV2 {
  	private ConcurrentMap<Integer, Long> lastOffsetPartitions;
    private volatile boolean stop;

    private volatile InputRow row = null;

    private final Iterator<LineIterator> lineIterators;
    private final StringInputRowParser parser;

    private LineIterator lineIterator = null;

    public FileIteratingFirehoseV2(
        Iterator<LineIterator> lineIterators,
        StringInputRowParser parser
    )
    {
      this.lineIterators = lineIterators;
      this.parser = parser;
    }
		@Override
    public void close() throws IOException
    {
			stop = true;
    }

		@Override
    public boolean advance()
    {
			if (stop) {
        return false;
      }

      nextMessage();
      return true;
    }

		@Override
    public InputRow currRow()
    {
			return row;
    }

		@Override
    public Committer makeCommitter()
    {
			final Map<Integer, Long> offsets = Maps.newHashMap(lastOffsetPartitions);//TODO no test on offset

      return new Committer()
      {
        @Override
        public Object getMetadata()
        {
          return offsets;
        }

        @Override
        public void run()
        {

        }
      };
    }

		@Override
    public void start() throws Exception
    {
			nextMessage();
    }
		private void nextMessage()
    {
			while ((lineIterator == null || !lineIterator.hasNext()) && lineIterators.hasNext()) {
	      lineIterator = lineIterators.next();
	    }

	    stop = !(lineIterator != null && lineIterator.hasNext());
	    try {
	      if (lineIterator == null || !lineIterator.hasNext()) {
	        // Close old streams, maybe.
	        if (lineIterator != null) {
	          lineIterator.close();
	        }

	        lineIterator = lineIterators.next();
	      }

	      row = parser.parse((String)lineIterator.next());//parser.parse(lineIterator.next());TODO
	    }
	    catch (Exception e) {
	      throw Throwables.propagate(e);
	    }
    }
  };
}
