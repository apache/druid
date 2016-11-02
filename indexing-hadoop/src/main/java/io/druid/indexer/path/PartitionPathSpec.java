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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexer.hadoop.FSSpideringIterator;
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
import java.util.regex.Pattern;

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

    FileSystem fs = basePath.getFileSystem(job.getConfiguration());
    Set<String> paths = Sets.newTreeSet();

    Pattern matchPattern = (getPartitionColumns() != null && !getPartitionColumns().isEmpty()) ? getMatchPattern() : null;

    for (Path indexingPath: indexingPaths) {
      Preconditions.checkArgument(indexingPath.toString().startsWith(basePath.toString()),
          "indexingPaths should be subdirectories of basePath but %s is not", indexingPath);

      Iterable<FileStatus> fsIterator = FSSpideringIterator.spiderIterable(fs, indexingPath);
      for (FileStatus fileStatus: fsIterator) {
        String path = fileStatus.getPath().toString();
        if (matchPattern == null || matchPattern.matcher(path).matches()) {
          paths.add(path);
        }
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
    if (dirPath.length() == basePath.length()) {
      return ImmutableMap.of();
    }

    String targetToFindValues = dirPath.substring(basePath.length() + 1);
    String[] partitions = targetToFindValues.split(Path.SEPARATOR);
    Map<String, String> values = Maps.newHashMap();
    for (String partition: partitions) {
      String[] keyValue = partition.split("=");
      if (keyValue.length == 2 && isPartitionColumn(keyValue[0])) {
        values.put(keyValue[0], keyValue[1]);
      }
    }

    return values;
  }

  private Pattern getMatchPattern()
  {
    Preconditions.checkNotNull(partitionColumns, "partitionColumns is null");

    StringBuilder partitionPattern = new StringBuilder();
    partitionPattern.append(".*").append(basePath).append(Path.SEPARATOR);
    for (String partitionColumn: partitionColumns) {
      partitionPattern.append(partitionColumn).append("=.*?").append(Path.SEPARATOR);
    }

    return Pattern.compile(partitionPattern.append(".*").toString().replace("/","\\/"));
  }

  private boolean isPartitionColumn(String columnName)
  {
    return partitionColumns == null || partitionColumns.contains(columnName);
  }
}
