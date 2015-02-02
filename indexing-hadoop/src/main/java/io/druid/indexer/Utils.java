/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import com.metamx.common.ISE;
import io.druid.jackson.DefaultObjectMapper;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 */
public class Utils
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  public static <K, V> Map<K, V> zipMap(Iterable<K> keys, Iterable<V> values)
  {
    Map<K, V> retVal = new HashMap<K, V>();

    Iterator<K> keyIter = keys.iterator();
    Iterator<V> valsIter = values.iterator();
    while (keyIter.hasNext()) {
      final K key = keyIter.next();

      Preconditions.checkArgument(valsIter.hasNext(), "keys longer than vals, bad, bad vals.  Broke on key[%s]", key);
      retVal.put(key, valsIter.next());
    }
    if (valsIter.hasNext()) {
      throw new ISE("More values[%d] than keys[%d]", retVal.size() + Iterators.size(valsIter), retVal.size());
    }

    return retVal;
  }

  public static OutputStream makePathAndOutputStream(JobContext job, Path outputPath, boolean deleteExisting)
      throws IOException
  {
    OutputStream retVal;
    FileSystem fs = outputPath.getFileSystem(job.getConfiguration());

    if (fs.exists(outputPath)) {
      if (deleteExisting) {
        fs.delete(outputPath, false);
      } else {
        throw new ISE("outputPath[%s] must not exist.", outputPath);
      }
    }

    if (!FileOutputFormat.getCompressOutput(job)) {
      retVal = fs.create(outputPath, false);
    } else {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      outputPath = new Path(outputPath.toString() + codec.getDefaultExtension());

      retVal = codec.createOutputStream(fs.create(outputPath, false));
    }

    return retVal;
  }

  public static InputStream openInputStream(JobContext job, Path inputPath) throws IOException
  {
    return openInputStream(job, inputPath, inputPath.getFileSystem(job.getConfiguration()));
  }

  public static boolean exists(JobContext job, FileSystem fs, Path inputPath) throws IOException
  {
    if (!FileOutputFormat.getCompressOutput(job)) {
      return fs.exists(inputPath);
    } else {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      return fs.exists(new Path(inputPath.toString() + codec.getDefaultExtension()));
    }
  }

  public static InputStream openInputStream(JobContext job, Path inputPath, final FileSystem fileSystem)
      throws IOException
  {
    if (!FileOutputFormat.getCompressOutput(job)) {
      return fileSystem.open(inputPath);
    } else {
      Class<? extends CompressionCodec> codecClass = FileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
      CompressionCodec codec = ReflectionUtils.newInstance(codecClass, job.getConfiguration());
      inputPath = new Path(inputPath.toString() + codec.getDefaultExtension());

      return codec.createInputStream(fileSystem.open(inputPath));
    }
  }

  public static Map<String, Object> getStats(JobContext job, Path statsPath)
      throws IOException
  {
    FileSystem fs = statsPath.getFileSystem(job.getConfiguration());

    return jsonMapper.readValue(
        fs.open(statsPath),
        new TypeReference<Map<String, Object>>()
        {
        }
    );
  }

  public static void storeStats(
      JobContext job, Path path, Map<String, Object> stats
  ) throws IOException
  {
    jsonMapper.writeValue(
        makePathAndOutputStream(job, path, true),
        stats
    );
  }
}
