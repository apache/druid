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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PartitionPathSpec implements PathSpec
{
  private static final Logger log = new Logger(PartitionPathSpec.class);

  private String basePath;
  private List<String> indexingPaths;
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
  public List<String> getIndexingPaths()
  {
    return indexingPaths;
  }

  public void setIndexingPaths(List<String> indexingPaths)
  {
    this.indexingPaths = indexingPaths;
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
    List<Path> indexingPaths = this.indexingPaths != null ?
        Lists.transform(
            this.indexingPaths,
            new Function<String, Path>() {
              @Nullable
              @Override
              public Path apply(@Nullable String input) {
                return new Path(input);
              }
            }
        )
        : ImmutableList.of(basePath);
    Preconditions.checkArgument(indexingPaths.toString().contains(basePath.toString()));

    FileSystem fs = basePath.getFileSystem(job.getConfiguration());
    Set<String> paths = Sets.newTreeSet();

    if (getPartitionColumns() != null) {
      for (Path indexingPath: indexingPaths) {
        log.info("Checking the directory recursively if it has the same name as the given partition column");
        int indexingStartColumnIndex = 0;

        // skip some partition columns for partial indexing of partitions
        if (basePath.toString().length() != indexingPath.toString().length())
        {
          String targetToFindSkipColumns = indexingPath.toString().substring(basePath.toString().length() + 1);
          String[] skipColumnValues = targetToFindSkipColumns.split(Path.SEPARATOR);
          Preconditions.checkArgument(skipColumnValues.length <= partitionColumns.size(),
              "partition columns should include all the columns specified in indexingPaths");

          for (String skipColumnValue: skipColumnValues)
          {
            String[] columnValuePair = skipColumnValue.split("=");
            Preconditions.checkArgument(columnValuePair.length == 2,
                String.format("%s: indexingPaths should not have non-partitioning directories", skipColumnValue));
            Preconditions.checkArgument(columnValuePair[0].equals(partitionColumns.get(indexingStartColumnIndex)));
            indexingStartColumnIndex++;
          }
        }

        // scan all the sub-directories under indexingPaths and add them to input path
        if (indexingStartColumnIndex == partitionColumns.size())
        {
          paths.add(fs.getFileStatus(indexingPath).getPath().toString());
        } else {
          Path[] pathToFilter = statusToPath(fs.listStatus(indexingPath, new PartitionPathFilter(partitionColumns.get(indexingStartColumnIndex))));

          for (int idx = indexingStartColumnIndex + 1; idx < partitionColumns.size(); idx++) {
            pathToFilter = statusToPath(fs.listStatus(pathToFilter, new PartitionPathFilter(partitionColumns.get(idx))));
          }

          for (Path path: pathToFilter) {
            paths.add(path.toString());
          }
        }
      }

    } else {
      log.info("Automatically find the partition columns from directory names");
      for (Path indexingPath: indexingPaths) {
        autoAddPath(paths, fs, indexingPath);
      }
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
      if (pathName.split("=").length != 2)
        return false;
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
        String[] split = child.getName().split("=");
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
