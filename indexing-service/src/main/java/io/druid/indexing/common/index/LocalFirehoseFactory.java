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

package io.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.StringInputRowParser;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

/**
 */
public class LocalFirehoseFactory implements FirehoseFactory
{
  private final File baseDir;
  private final String filter;
  private final StringInputRowParser parser;

  @JsonCreator
  public LocalFirehoseFactory(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter,
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
  public Firehose connect() throws IOException
  {
    return new FileIteratingFirehose<File>(
        Lists.<File>newLinkedList(
            Arrays.<File>asList(
                baseDir.listFiles(
                    new FilenameFilter()
                    {
                      @Override
                      public boolean accept(File file, String name)
                      {
                        return name.contains(filter);
                      }
                    }
                )
            )
        ),
        parser
    )
    {
      @Override
      public LineIterator makeLineIterator(File file) throws Exception
      {
        return FileUtils.lineIterator(file);
      }
    };
  }
}
