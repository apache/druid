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

package io.druid.firehose.azure;

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
import io.druid.storage.azure.AzureByteSource;
import io.druid.storage.azure.AzureStorage;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is heavily inspired by the StaticS3FirehoseFactory class in the io.druid.firehose.s3 package
 */
public class StaticAzureBlobStoreFirehoseFactory implements FirehoseFactory<StringInputRowParser> {
  private static final Logger log = new Logger(StaticAzureBlobStoreFirehoseFactory.class);

  private final AzureStorage azureStorage;
  private final List<AzureBlob> blobs;

  @JsonCreator
  public StaticAzureBlobStoreFirehoseFactory(
      @JacksonInject("azureStorage") AzureStorage azureStorage,
      @JsonProperty("blobs") AzureBlob[] blobs
  ) {
    this.azureStorage = azureStorage;
    this.blobs = ImmutableList.copyOf(blobs);
  }

  @JsonProperty
  public List<AzureBlob> getBlobs() {
    return blobs;
  }

  @Override
  public Firehose connect(StringInputRowParser stringInputRowParser) throws IOException {
    Preconditions.checkNotNull(azureStorage, "null azureStorage");

    final LinkedList<AzureBlob> objectQueue = Lists.newLinkedList(blobs);

    return new FileIteratingFirehose(
        new Iterator<LineIterator>() {
          @Override
          public boolean hasNext() {
            return !objectQueue.isEmpty();
          }

          @Override
          public LineIterator next() {
            final AzureBlob nextURI = objectQueue.poll();

            final String container = nextURI.getContainer();
            final String path = nextURI.getPath().startsWith("/")
                ? nextURI.getPath().substring(1)
                : nextURI.getPath();

            try {
              final InputStream innerInputStream = new AzureByteSource(azureStorage, container, path).openStream();


              final InputStream outerInputStream = path.endsWith(".gz")
                  ? CompressionUtils.gzipInputStream(innerInputStream)
                  : innerInputStream;

              return IOUtils.lineIterator(
                  new BufferedReader(
                      new InputStreamReader(outerInputStream, Charsets.UTF_8)
                  )
              );
            } catch (Exception e) {
              log.error(e,
                  "Exception opening container[%s] blob[%s]",
                  container,
                  path
              );

              throw Throwables.propagate(e);
            }
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        },
        stringInputRowParser
    );
  }
}
