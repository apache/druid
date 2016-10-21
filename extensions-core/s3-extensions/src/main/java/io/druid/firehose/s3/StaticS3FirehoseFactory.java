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

package io.druid.firehose.s3;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.impl.FileIteratingFirehose;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;

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

/**
 * Builds firehoses that read from a predefined list of S3 objects and then dry up.
 */
public class StaticS3FirehoseFactory implements FirehoseFactory<StringInputRowParser>
{
  private static final Logger log = new Logger(StaticS3FirehoseFactory.class);

  private final RestS3Service s3Client;
  private final List<URI> uris;

  @JsonCreator
  public StaticS3FirehoseFactory(
      @JacksonInject("s3Client") RestS3Service s3Client,
      @JsonProperty("uris") List<URI> uris
  )
  {
    this.s3Client = s3Client;
    this.uris = ImmutableList.copyOf(uris);

    for (final URI inputURI : uris) {
      Preconditions.checkArgument(inputURI.getScheme().equals("s3"), "input uri scheme == s3 (%s)", inputURI);
    }
  }

  @JsonProperty
  public List<URI> getUris()
  {
    return uris;
  }

  @Override
  public Firehose connect(StringInputRowParser firehoseParser) throws IOException
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
                  new S3Bucket(s3Bucket), s3Object.getKey()
              )
                                                           .getDataInputStream();

              final InputStream outerInputStream = s3Object.getKey().endsWith(".gz")
                                                   ? CompressionUtils.gzipInputStream(innerInputStream)
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
        firehoseParser
    );
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    StaticS3FirehoseFactory factory = (StaticS3FirehoseFactory) o;

    return !(uris != null ? !uris.equals(factory.uris) : factory.uris != null);

  }

  @Override
  public int hashCode()
  {
    return uris != null ? uris.hashCode() : 0;
  }
}
