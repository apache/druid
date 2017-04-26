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

package io.druid.indexer.hadoop;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import javax.validation.constraints.NotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class DatasourceInputSplit extends InputSplit implements Writable
{
  private static final String[] EMPTY_STR_ARRAY = new String[0];

  private List<WindowedDataSegment> segments = null;
  private String[] locations = null;

  //required for deserialization
  public DatasourceInputSplit()
  {
  }

  public DatasourceInputSplit(@NotNull List<WindowedDataSegment> segments, String[] locations)
  {
    Preconditions.checkArgument(segments != null && segments.size() > 0, "no segments");
    this.segments = segments;
    this.locations = locations == null ? EMPTY_STR_ARRAY : locations;
  }

  @Override
  public long getLength() throws IOException, InterruptedException
  {
    long size = 0;
    for (WindowedDataSegment segment : segments) {
      size += segment.getSegment().getSize();
    }
    return size;
  }

  @Override
  public String[] getLocations() throws IOException, InterruptedException
  {
    return locations;
  }

  public List<WindowedDataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public void write(DataOutput out) throws IOException
  {
    final byte[] segmentsBytes = HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsBytes(segments);
    out.writeInt(segmentsBytes.length);
    out.write(segmentsBytes);
    out.writeInt(locations.length);
    for (String location : locations) {
      out.writeUTF(location);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException
  {
    final int segmentsBytesLength = in.readInt();
    final byte[] buf = new byte[segmentsBytesLength];
    in.readFully(buf);
    segments = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(
        buf,
        new TypeReference<List<WindowedDataSegment>>()
        {
        }
    );
    locations = new String[in.readInt()];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = in.readUTF();
    }
  }
}
