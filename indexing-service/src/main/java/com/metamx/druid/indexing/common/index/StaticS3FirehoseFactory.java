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

package com.metamx.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.indexer.data.StringInputRowParser;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.realtime.firehose.Firehose;
import com.metamx.druid.realtime.firehose.FirehoseFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import org.jets3t.service.S3Service;
import org.jets3t.service.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.zip.GZIPInputStream;

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
@JsonTypeName("s3")
public class StaticS3FirehoseFactory implements FirehoseFactory
{
  private final S3Service s3Client;
  private final StringInputRowParser parser;
  private final List<URI> uris;

  private final long retryCount = 5;
  private final long retryMillis = 5000;

  private static final Logger log = new Logger(StaticS3FirehoseFactory.class);

  @JsonCreator
  public StaticS3FirehoseFactory(
      @JacksonInject("s3Client") S3Service s3Client,
      @JsonProperty("parser") StringInputRowParser parser,
      @JsonProperty("uris") List<URI> uris
  )
  {
    this.s3Client = s3Client;
    this.parser = Preconditions.checkNotNull(parser, "parser");
    this.uris = ImmutableList.copyOf(uris);

    for (final URI inputURI : uris) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
    }
  }

  @JsonProperty
  public StringInputRowParser getParser()
  {
    return parser;
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @Override
  public Firehose connect() throws IOException
  {
    Preconditions.checkNotNull(s3Client, "null s3Client");

    return new Firehose()
    {
      LineIterator lineIterator = null;
      final Queue<URI> objectQueue = Lists.newLinkedList(uris);

      // Rolls over our streams and iterators to the next file, if appropriate
      private void maybeNextFile() throws Exception
      {

        if (lineIterator == null || !lineIterator.hasNext()) {

          // Close old streams, maybe.
          if (lineIterator != null) {
            lineIterator.close();
          }

          // Open new streams, maybe.
          final URI nextURI = objectQueue.poll();
          if (nextURI != null) {

            final String s3Bucket = nextURI.getAuthority();
            final S3Object s3Object = new S3Object(
                nextURI.getPath().startsWith("/")
                ? nextURI.getPath().substring(1)
                : nextURI.getPath()
            );

            log.info("Reading from bucket[%s] object[%s] (%s)", s3Bucket, s3Object.getKey(), nextURI);

            int ntry = 0;
            try {
              final InputStream innerInputStream = s3Client.getObject(s3Bucket, s3Object.getKey())
                                                           .getDataInputStream();

              final InputStream outerInputStream = s3Object.getKey().endsWith(".gz")
                                                   ? new GZIPInputStream(innerInputStream)
                                                   : innerInputStream;

              lineIterator = IOUtils.lineIterator(
                  new BufferedReader(
                      new InputStreamReader(outerInputStream, Charsets.UTF_8)
                  )
              );
            } catch(IOException e) {
              log.error(
                  e,
                  "Exception reading from bucket[%s] object[%s] (try %d) (sleeping %d millis)",
                  s3Bucket,
                  s3Object.getKey(),
                  ntry,
                  retryMillis
              );

              ntry ++;
              if(ntry <= retryCount) {
                Thread.sleep(retryMillis);
              }
            }

          }
        }

      }

      @Override
      public boolean hasMore()
      {
        try {
          maybeNextFile();
        } catch(Exception e) {
          throw Throwables.propagate(e);
        }

        return lineIterator != null && lineIterator.hasNext();
      }

      @Override
      public InputRow nextRow()
      {
        try {
          maybeNextFile();
        } catch(Exception e) {
          throw Throwables.propagate(e);
        }

        if(lineIterator == null) {
          throw new NoSuchElementException();
        }

        return parser.parse(lineIterator.next());
      }

      @Override
      public Runnable commit()
      {
        // Do nothing.
        return new Runnable() { public void run() {} };
      }

      @Override
      public void close() throws IOException
      {
        objectQueue.clear();
        if(lineIterator != null) {
          lineIterator.close();
        }
      }
    };
  }
}
