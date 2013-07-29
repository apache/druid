/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package druid.examples.flights;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import com.google.common.io.Closeables;
import com.metamx.druid.indexer.data.StringInputRowParser;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;

/**
 */
public class FlightsFirehoseFactory implements FirehoseFactory
{
  private final String flightsFileLocation;
  private final StringInputRowParser parser;

  @JsonCreator
  public FlightsFirehoseFactory(
      @JsonProperty("directory") String flightsFilesDirectory,
      @JsonProperty("parser") StringInputRowParser parser
  )
  {
    this.flightsFileLocation = flightsFilesDirectory;
    this.parser = parser;
  }

  @JsonProperty("directory")
  public String getFlightsFileLocation()
  {
    return flightsFileLocation;
  }

  @Override
  public Firehose connect() throws IOException
  {
    File dir = new File(flightsFileLocation);

    final Iterator<File> files = Iterators.forArray(dir.listFiles());

    return new Firehose()
    {
      BufferedReader in = null;
      String line = null;

      @Override
      public boolean hasMore()
      {
        try {
          if (line != null) {
            return true;
          }
          else if (in != null) {
            line = in.readLine();

            if (line == null) {
              Closeables.closeQuietly(in);
              in = null;
            }

            return true;
          }
          else if (files.hasNext()) {
            final File nextFile = files.next();

            if (nextFile.getName().endsWith(".gz")) {
              in = new BufferedReader(
                  new InputStreamReader(new GZIPInputStream(new FileInputStream(nextFile)), Charsets.UTF_8)
              );
            }
            else {
              in = new BufferedReader(new FileReader(nextFile));
            }
            return hasMore();
          }
          Thread.currentThread().join();
          return false;
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
        catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      }

      @Override
      public InputRow nextRow()
      {
        final InputRow retVal = parser.parse(line);
        line = null;
        return retVal;
      }

      @Override
      public Runnable commit()
      {
        return new Runnable()
        {
          @Override
          public void run()
          {

          }
        };
      }

      @Override
      public void close() throws IOException
      {
        Closeables.closeQuietly(in);
      }
    };
  }
}
