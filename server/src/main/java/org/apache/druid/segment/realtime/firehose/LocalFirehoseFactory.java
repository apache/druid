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

package org.apache.druid.segment.realtime.firehose;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.druid.data.input.FiniteFirehoseFactory;
import org.apache.druid.data.input.InputSplit;
import org.apache.druid.data.input.impl.AbstractTextFilesFirehoseFactory;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.utils.CompressionUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

/**
 * Firehose that reads data from files on local disk
 */
public class LocalFirehoseFactory extends AbstractTextFilesFirehoseFactory<File>
{
  private static final EmittingLogger log = new EmittingLogger(LocalFirehoseFactory.class);

  private final File baseDir;
  private final String filter;
  @Nullable
  private final StringInputRowParser parser;

  @JsonCreator
  public LocalFirehoseFactory(
      @JsonProperty("baseDir") File baseDir,
      @JsonProperty("filter") String filter,
      // Backwards compatible
      @Nullable @JsonProperty("parser") StringInputRowParser parser
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
  @Nullable
  public StringInputRowParser getParser()
  {
    return parser;
  }

  @Override
  protected Collection<File> initObjects()
  {
    final Collection<File> files = FileUtils.listFiles(
        Preconditions.checkNotNull(baseDir).getAbsoluteFile(),
        new WildcardFileFilter(filter),
        TrueFileFilter.INSTANCE
    );
    log.info("Initialized with " + files + " files");
    return files;
  }

  @Override
  protected InputStream openObjectStream(File object) throws IOException
  {
    return FileUtils.openInputStream(object);
  }

  @Override
  protected InputStream wrapObjectStream(File object, InputStream stream) throws IOException
  {
    return CompressionUtils.decompress(stream, object.getPath());
  }

  @Override
  public FiniteFirehoseFactory<StringInputRowParser, File> withSplit(InputSplit<File> split)
  {
    final File newFile = split.get();
    final File baseDir = newFile.getParentFile();
    final String filter = newFile.getName();
    return new LocalFirehoseFactory(baseDir, filter, parser);
  }
}
