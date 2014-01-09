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

package io.druid.firehose.s3;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.StringInputRowParser;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
public class StaticS3FirehoseFactory implements FirehoseFactory
{
  private static final Logger log = new Logger(StaticS3FirehoseFactory.class);

  private final RestS3Service s3Client;
  private final StringInputRowParser parser;
  private final List<URI> uris;

  @JsonCreator
  public StaticS3FirehoseFactory(
      @JacksonInject("s3Client") RestS3Service s3Client,
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

    final LinkedList<URI> objectQueue = Lists.newLinkedList(uris);

    return new FileIteratingFirehose(
        new Iterator<LineIterator>()
        {
          @Override
          public boolean hasNext()
          {
            return !objectQueue.isEmpty();
          }

          @Override
          public LineIterator next()
          {
            final URI nextURI = objectQueue.poll();

            final String s3Bucket = nextURI.getAuthority();
            final S3Object s3Object = new S3Object(
                nextURI.getPath().startsWith("/")
                ? nextURI.getPath().substring(1)
                : nextURI.getPath()
            );

            log.info("Reading from bucket[%s] object[%s] (%s)", s3Bucket, s3Object.getKey(), nextURI);

            try {
              final InputStream innerInputStream = s3Client.getObject(
                  new S3Bucket(s3Bucket), s3Object.getKey())
                      .getDataInputStream();

              final InputStream outerInputStream = s3Object.getKey().endsWith(".gz")
                                                   ? new GZIPInputStream(innerInputStream)
                                                   : innerInputStream;

              return IOUtils.lineIterator(
                  new BufferedReader(
                      new InputStreamReader(outerInputStream, Charsets.UTF_8)
                  )
              );
            }
            catch (Exception e) {
              log.error(
                  e,
                  "Exception reading from bucket[%s] object[%s]",
                  s3Bucket,
                  s3Object.getKey()
              );

              throw Throwables.propagate(e);
            }
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        },
        parser
    );
  }
}
