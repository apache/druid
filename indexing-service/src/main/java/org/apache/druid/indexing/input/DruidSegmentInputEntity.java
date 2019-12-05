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

package org.apache.druid.indexing.input;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import org.apache.druid.data.input.InputEntity;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.SegmentLoader;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.InputStream;
import java.net.URI;

public class DruidSegmentInputEntity implements InputEntity
{
  private static final EmittingLogger log = new EmittingLogger(DruidSegmentInputEntity.class);

  private final SegmentLoader segmentLoader;
  private final DataSegment segment;
  private final Interval intervalFilter;

  DruidSegmentInputEntity(SegmentLoader segmentLoader, DataSegment segment, Interval intervalFilter)
  {
    this.segmentLoader = segmentLoader;
    this.segment = segment;
    this.intervalFilter = intervalFilter;
  }

  Interval getIntervalFilter()
  {
    return intervalFilter;
  }

  @Nullable
  @Override
  public URI getUri()
  {
    return null;
  }

  @Override
  public InputStream open()
  {
    throw new UnsupportedOperationException("Don't call this");
  }

  @Override
  public CleanableFile fetch(File temporaryDirectory, byte[] fetchBuffer)
  {
    final File segmentFile;
    try {
      segmentFile = segmentLoader.getSegmentFiles(segment);
    }
    catch (SegmentLoadingException e) {
      throw new RuntimeException(e);
    }
    return new CleanableFile()
    {
      @Override
      public File file()
      {
        return segmentFile;
      }

      @Override
      public void close()
      {
        if (!segmentFile.delete()) {
          log.warn("Could not clean temporary segment file: " + segmentFile);
        }
      }
    };
  }

  @Override
  public Predicate<Throwable> getRetryCondition()
  {
    return Predicates.alwaysFalse();
  }
}
