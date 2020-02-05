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

package org.apache.druid.segment;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/**
 * Utility methods useful for implementing deep storage extensions.
 */
@PublicApi
public class SegmentUtils
{
  @VisibleForTesting
  static final int SEGMENTS_PER_LOG_MESSAGE = 64;

  private static final HashFunction HASH_FUNCTION = Hashing.sha256();

  /**
   * Hash the IDs of the given segments based on SHA-256 algorithm.
   */
  public static String hashIds(List<DataSegment> segments)
  {
    Collections.sort(segments);
    final Hasher hasher = HASH_FUNCTION.newHasher();
    segments.forEach(segment -> hasher.putString(segment.getId().toString(), StandardCharsets.UTF_8));
    return StringUtils.fromUtf8(hasher.hash().asBytes());
  }

  public static int getVersionFromDir(File inDir) throws IOException
  {
    File versionFile = new File(inDir, "version.bin");
    if (versionFile.exists()) {
      return Ints.fromByteArray(Files.toByteArray(versionFile));
    }

    final File indexFile = new File(inDir, "index.drd");
    int version;
    if (indexFile.exists()) {
      try (InputStream in = new FileInputStream(indexFile)) {
        version = in.read();
      }
      return version;
    }

    throw new IOE("Invalid segment dir [%s]. Can't find either of version.bin or index.drd.", inDir);
  }

  /**
   * Returns an object whose toString() returns a String with identifiers of the given segments, comma-separated. Useful
   * for log messages. Not useful for anything else, because this doesn't take special effort to escape commas that
   * occur in identifiers (not common, but could potentially occur in a datasource name).
   */
  @Nullable
  public static Object commaSeparatedIdentifiers(@Nullable final Collection<DataSegment> segments)
  {
    if (segments == null || segments.isEmpty()) {
      return null;
    }
    // Lazy, to avoid preliminary string creation if logging level is turned off
    return Collections2.transform(segments, DataSegment::getId);
  }

  /**
   * Logs all the segment ids you could ever want, {@link #SEGMENTS_PER_LOG_MESSAGE} at a time, as a comma separated
   * list.
   */
  public static void logSegments(
      Logger.LogFunction logger,
      @Nullable final Collection<DataSegment> segments,
      @Nullable String preamble
  )
  {
    if (segments == null || segments.isEmpty()) {
      return;
    }
    logSegmentIds(logger, segments.stream().map(DataSegment::getId), preamble);
  }

  /**
   * Logs all the segment ids you could ever want, {@link #SEGMENTS_PER_LOG_MESSAGE} at a time, as a comma separated
   * list.
   */
  public static void logSegmentIds(
      Logger.LogFunction logger,
      @Nullable final Stream<SegmentId> stream,
      @Nullable String preamble
  )
  {
    Preconditions.checkNotNull(preamble);
    if (stream == null) {
      return;
    }
    final Iterator<SegmentId> iterator = stream.iterator();
    if (!iterator.hasNext()) {
      return;
    }
    final String logFormat = preamble + ": %s";

    int counter = 0;
    StringBuilder sb = null;
    while (iterator.hasNext()) {
      SegmentId nextId = iterator.next();
      if (counter == 0) {
        // use segmentId string length of first as estimate for total size of builder for this batch
        sb = new StringBuilder(SEGMENTS_PER_LOG_MESSAGE * (2 + nextId.safeUpperLimitOfStringSize())).append("[");
      }
      sb.append(nextId);
      if (++counter < SEGMENTS_PER_LOG_MESSAGE && iterator.hasNext()) {
        sb.append(", ");
      }
      counter = counter % SEGMENTS_PER_LOG_MESSAGE;
      if (counter == 0) {
        // flush
        sb.append("]");
        logger.log(logFormat, sb.toString());
      }
    }

    // check for stragglers
    if (counter > 0) {
      sb.append("]");
      logger.log(logFormat, sb.toString());
    }
  }

  private SegmentUtils()
  {

  }
}
