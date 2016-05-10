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

package io.druid.indexer.path;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionPathSpec implements PathSpec
{
  private static final Logger log = new Logger(PartitionPathSpec.class);

  private String basePath;
  private List<String> partitionColumns;
  private Class<? extends InputFormat> inputFormat;

  @JsonProperty
  public String getBasePath()
  {
    return basePath;
  }

  public void setBasePath(String basePath)
  {
    this.basePath = basePath;
  }

  @JsonProperty
  public List<String> getPartitionColumns()
  {
    return partitionColumns;
  }

  public void setPartitionColumns(List<String> partitionColumns)
  {
    this.partitionColumns = partitionColumns;
  }

  @JsonProperty
  public Class<? extends InputFormat> getInputFormat()
  {
    return inputFormat;
  }

  public void setInputFormat(Class<? extends InputFormat> inputFormat)
  {
    this.inputFormat = inputFormat;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    Path basePath = new Path(Preconditions.checkNotNull(this.basePath));
    FileSystem fs = basePath.getFileSystem(job.getConfiguration());
    Set<String> paths = Sets.newTreeSet();

    if (getPartitionColumns() != null) {
      log.info("Checking the directory recursively as partition columns are given");
      Path[] pathToFilter = statusToPath(fs.listStatus(basePath, new PartitionPathFilter(partitionColumns.get(0))));

      for (int idx = 1; idx < partitionColumns.size(); idx++) {
        pathToFilter = statusToPath(fs.listStatus(pathToFilter, new PartitionPathFilter(partitionColumns.get(idx))));
      }

      for (Path path: pathToFilter) {
        paths.add(path.toString());
      }
    } else {
      log.info("Automatically find the partition columns from directory names");
      autoAddPath(paths, fs, basePath);
    }

    log.info("Appending path %s", paths);
    StaticPathSpec.addToMultipleInputs(config, job, paths, inputFormat);

    return job;
  }

  public Map<String, String> getPartitionValues(Path path)
  {
    String dirPath = path.getParent().toUri().getPath();
    Preconditions.checkArgument(dirPath.startsWith(basePath));
    if (dirPath.length() == basePath.length())
    {
      return ImmutableMap.of();
    }

    String targetToFindValues = dirPath.substring(basePath.length() + 1);
    String[] partitions = targetToFindValues.split(Path.SEPARATOR);
    Map<String, String> values = Maps.newHashMap();
    for (String partition: partitions) {
      String[] keyValue = partition.split("=");
      if (keyValue.length == 2)
      {
        values.put(keyValue[0], keyValue[1]);
      }
    }

    return values;
  }

  private Path[] statusToPath(FileStatus[] statuses)
  {
    List<Path> dirPath = Lists.newArrayListWithExpectedSize(statuses.length);
    for (FileStatus status: statuses) {
      if (status.isDirectory()) {
        dirPath.add(status.getPath());
      }
    }

    return dirPath.toArray(new Path[dirPath.size()]);
  }

  private class PartitionPathFilter implements PathFilter
  {
    final String shouldBeStartWith;

    public PartitionPathFilter(
        String columnName
    )
    {
      shouldBeStartWith = columnName + "=";
    }

    @Override
    public boolean accept(Path path)
    {
      String pathName = path.getName();
      return pathName.startsWith(shouldBeStartWith);
    }
  }

  private void autoAddPath(Set<String> paths, FileSystem fs, Path path) throws IOException
  {
    boolean hasFile = false;
    for (FileStatus fileStatus: fs.listStatus(path))
    {
      Path child = fileStatus.getPath();
      if (fileStatus.isDirectory()) {
        String[] split = child.toString().split("=");
        if (split.length == 2) {
          autoAddPath(paths, fs, child);
        }
      } else {
        hasFile = true;
      }
    }

    if (hasFile)
      paths.add(path.toString());
  }
}
