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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.Objects;

/**
 * holds a {@link DataSegment} with the temporary file path where the corresponding index zip file is currently stored
 * and the final path where the index zip file should eventually be moved to.
 * see {@link JobHelper#renameIndexFilesForSegments(HadoopIngestionSpec, List)}
 */
public class DataSegmentAndIndexZipFilePath
{
  private final DataSegment segment;
  private final String tmpIndexZipFilePath;
  private final String finalIndexZipFilePath;

  @JsonCreator
  public DataSegmentAndIndexZipFilePath(
      @JsonProperty("segment") DataSegment segment,
      @JsonProperty("tmpIndexZipFilePath") String tmpIndexZipFilePath,
      @JsonProperty("finalIndexZipFilePath") String finalIndexZipFilePath
  )
  {
    this.segment = segment;
    this.tmpIndexZipFilePath = tmpIndexZipFilePath;
    this.finalIndexZipFilePath = finalIndexZipFilePath;
  }

  @JsonProperty
  public DataSegment getSegment()
  {
    return segment;
  }

  @JsonProperty
  public String getTmpIndexZipFilePath()
  {
    return tmpIndexZipFilePath;
  }

  @JsonProperty
  public String getFinalIndexZipFilePath()
  {
    return finalIndexZipFilePath;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o instanceof DataSegmentAndIndexZipFilePath) {
      DataSegmentAndIndexZipFilePath that = (DataSegmentAndIndexZipFilePath) o;
      return segment.equals(((DataSegmentAndIndexZipFilePath) o).getSegment())
          && tmpIndexZipFilePath.equals(that.getTmpIndexZipFilePath())
          && finalIndexZipFilePath.equals(that.getFinalIndexZipFilePath());
    }
    return false;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(segment.getId(), tmpIndexZipFilePath);
  }

  @Override
  public String toString()
  {
    return "DataSegmentAndIndexZipFilePath{" +
           "segment=" + segment +
           ", tmpIndexZipFilePath=" + tmpIndexZipFilePath +
           ", finalIndexZipFilePath=" + finalIndexZipFilePath +
           '}';
  }
}
