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

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.druid.common.utils.PropUtils;
import io.druid.data.output.CountingAccumulator;
import io.druid.data.output.Formatters;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.ResultWriter;
import io.druid.query.TabularFormat;
import io.druid.segment.SegmentUtils;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

/**
 */
public class LocalDataSegmentPusher implements DataSegmentPusher, ResultWriter
{
  private static final Logger log = new Logger(LocalDataSegmentPusher.class);

  private final LocalDataSegmentPusherConfig config;
  private final ObjectMapper jsonMapper;

  @Inject
  public LocalDataSegmentPusher(
      LocalDataSegmentPusherConfig config,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;

    log.info("Configured local filesystem as deep storage");
  }

  @Override
  public String getPathForHadoop()
  {
    return config.getStorageDirectory().getAbsoluteFile().toURI().toString();
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public DataSegment push(File dataSegmentFile, DataSegment segment) throws IOException
  {
    File outDir = new File(config.getStorageDirectory(), DataSegmentPusherUtil.getStorageDir(segment));

    log.info("Copying segment[%s] to local filesystem at location[%s]", segment.getIdentifier(), outDir.toString());

    if (dataSegmentFile.equals(outDir)) {
      long size = 0;
      for (File file : dataSegmentFile.listFiles()) {
        size += file.length();
      }

      return createDescriptorFile(
          segment.withLoadSpec(makeLoadSpec(outDir))
                 .withSize(size)
                 .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
          outDir
      );
    }

    if (!outDir.mkdirs() && !outDir.isDirectory()) {
      throw new IOException(String.format("Cannot create directory[%s]", outDir));
    }
    File outFile = new File(outDir, "index.zip");
    log.info("Compressing files from[%s] to [%s]", dataSegmentFile, outFile);
    long size = CompressionUtils.zip(dataSegmentFile, outFile);

    return createDescriptorFile(
        segment.withLoadSpec(makeLoadSpec(outFile))
               .withSize(size)
               .withBinaryVersion(SegmentUtils.getVersionFromDir(dataSegmentFile)),
        outDir
    );
  }

  private DataSegment createDescriptorFile(DataSegment segment, File outDir) throws IOException
  {
    File descriptorFile = new File(outDir, "descriptor.json");
    log.info("Creating descriptor file at[%s]", descriptorFile);
    Files.copy(ByteStreams.newInputStreamSupplier(jsonMapper.writeValueAsBytes(segment)), descriptorFile);
    return segment;
  }

  private ImmutableMap<String, Object> makeLoadSpec(File outFile)
  {
    return ImmutableMap.<String, Object>of("type", "local", "path", outFile.toString());
  }

  @Override
  public Map<String, Object> write(URI location, final TabularFormat result, final Map<String, Object> context)
      throws IOException
  {
    File targetDirectory = new File(location.getPath());
    boolean cleanup = PropUtils.parseBoolean(context, "cleanup", false);
    if (cleanup) {
      FileUtils.deleteDirectory(targetDirectory);
    }
    if (targetDirectory.isFile()) {
      throw new IllegalStateException("'resultDirectory' should not be a file");
    }
    if (!targetDirectory.exists() && !targetDirectory.mkdirs()) {
      throw new IllegalStateException("failed to make target directory");
    }
    String fileName = PropUtils.parseString(context, "dataFileName", null);
    File dataFile = new File(targetDirectory, Strings.isNullOrEmpty(fileName) ? "data" : fileName);

    Map<String, Object> info = Maps.newHashMap();
    try (OutputStream output = new BufferedOutputStream(new FileOutputStream(dataFile))) {
      CountingAccumulator accumulator = Formatters.toExporter(context, output, jsonMapper);
      try {
        accumulator.begin(output);
        result.getSequence().accumulate(null, accumulator);
        info.put("numRows", accumulator.count());
      }
      finally {
        accumulator.end(output);
      }
    }
    info.put(rewrite(location, dataFile.getAbsolutePath()), dataFile.length());

    Map<String, Object> metaData = result.getMetaData();
    if (metaData != null && !metaData.isEmpty()) {
      File metaFile = new File(targetDirectory, ".meta");
      try (OutputStream output = new FileOutputStream(metaFile)) {
        jsonMapper.writeValue(output, metaData);
      }
      info.put(rewrite(location, metaFile.getAbsolutePath()), metaFile.length());
    }
    return info;
  }

  private String rewrite(URI location, String path) {
    try {
      return new URI(
          location.getScheme(),
          location.getUserInfo(),
          location.getHost(),
          location.getPort(),
          path,
          null,
          null
      ).toString();
    }
    catch (URISyntaxException e) {
      return path;
    }
  }
}
