/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.StringInputRowParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 */
public class LocalFirehoseFactory implements FirehoseFactory<StringInputRowParser>
{
  private static final EmittingLogger log = new EmittingLogger(LocalFirehoseFactory.class);

  private final File baseDir;
  private final String filter;
  private final StringInputRowParser parser;

  @JsonCreator
  public LocalFirehoseFactory(
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
  public Firehose connect(StringInputRowParser firehoseParser) throws IOException
  {
    log.info("Searching for all [%s] in [%s]", filter, baseDir.getAbsoluteFile());

    Collection<File> foundFiles = FileUtils.listFiles(
        baseDir.getAbsoluteFile(),
        new WildcardFileFilter(filter),
        TrueFileFilter.INSTANCE
    );

    if (foundFiles == null || foundFiles.isEmpty()) {
      throw new ISE("Found no files to ingest! Check your schema.");
    }

    final LinkedList<File> files = Lists.newLinkedList(
        foundFiles
    );

    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
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
        },
        firehoseParser
    );
  }
}
