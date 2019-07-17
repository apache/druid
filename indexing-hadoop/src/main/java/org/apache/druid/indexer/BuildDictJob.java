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

import com.google.common.base.Optional;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.dict.AppendTrieDictionary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class BuildDictJob implements Jobby
{
  private static final Logger log = new Logger(BuildDictJob.class);

  private final HadoopDruidIndexerConfig config;
  private final String zkHosts;
  private final String zkBase;

  public BuildDictJob(HadoopDruidIndexerConfig config, String zkHosts, String zkBase)
  {
    this.config = config;
    this.zkHosts = zkHosts;
    this.zkBase = zkBase;
  }

  @Override
  public boolean run()
  {
    try {
      final long startTime = System.currentTimeMillis();

      String skipDict = config.getSchema().getTuningConfig().getJobProperties().getOrDefault("druid.dict.skip", "false");
      if (skipDict != null && "true".equals(skipDict)) {
        log.info("skipDict set true, no need to build dict");
        return true;
      }

      Job job = Job.getInstance(
          new Configuration(),
          StringUtils.format("%s-build-dict-%s", config.getDataSource(), config.getIntervals())
      );

      //8G memory is enough for all global dict, because the input is sequential and we handle global dict slice by slice
      //can be override by jobproperty
      job.getConfiguration().set("mapreduce.reduce.memory.mb", "8500");
      job.getConfiguration().set("mapred.reduce.child.java.opts", "-Xmx8g");

      job.getConfiguration().set(HadoopDruidIndexerConfig.CONFIG_PROPERTY + ".zkhosts", zkHosts);
      job.getConfiguration().set(HadoopDruidIndexerConfig.CONFIG_PROPERTY + ".zkbase", zkBase);


      JobHelper.injectSystemProperties(job);
      config.addJobProperties(job);
      // inject druid properties like deep storage bindings
      JobHelper.injectDruidProperties(job.getConfiguration(), config.getAllowedHadoopPrefix());

      job.setMapperClass(BuildDictMapper.class);
      job.setMapOutputKeyClass(SelfDefineSortableKey.class);
      job.setMapOutputValueClass(NullWritable.class);

      job.setCombinerClass(BuildDictCombiner.class);

      job.setReducerClass(BuildDictReducer.class);
      job.setPartitionerClass(BuildDictPartitioner.class);
      int reducerCount = 0;
      AggregatorFactory[] aggregators = config.getSchema().getDataSchema().getAggregators();
      for (AggregatorFactory aggFactory : aggregators) {
        if (aggFactory.getTypeName().equals("unique")) {
          reducerCount++;
        }
      }

      if (reducerCount == 0) {
        log.info("no unique metrics, no need to build dict");
        return true;
      }
      job.setNumReduceTasks(reducerCount);

      job.setOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(NullWritable.class);

      //prevent to create zero-sized default output
      LazyOutputFormat.setOutputFormatClass(job, SequenceFileOutputFormat.class);

      FileOutputFormat.setOutputPath(job, new Path(config.makeIntermediatePath(), "dictOutput"));



      job.getConfiguration().set("mapred.output.compress", "false"); //TODO use hadoop constant instead


      //Copying global dict to working dir in GlobalDictHDFSStore maybe elapsed a long time (Maybe we could improve it)
      //Waiting the global dict lock maybe also take a long time.
      //So we set 8 hours here
      job.getConfiguration().set("mapreduce.task.timeout", "28800000");

      config.addInputPaths(job);
      config.intoConfiguration(job);
      JobHelper.setupClasspath(
          JobHelper.distributedClassPath(config.getWorkingPath()),
          JobHelper.distributedClassPath(config.makeIntermediatePath()),
          job
      );

      job.submit();
      log.info("Job %s submitted, status available at %s", job.getJobName(), job.getTrackingURL());

      boolean success = job.waitForCompletion(true);

      log.info(
          "BuildDictJob took %d millis",
          (System.currentTimeMillis() - startTime)
      );
      return success;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class BuildDictMapper extends HadoopDruidIndexerMapper<SelfDefineSortableKey, NullWritable>
  {
    private List<AggregatorFactory> uniqAggregators;

    protected Text outputKey = new Text();
    private ByteBuffer tmpBuf;
    private SelfDefineSortableKey sortableKey = new SelfDefineSortableKey();

    @Override
    protected void setup(Mapper.Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);
      tmpBuf = ByteBuffer.allocate(4096);

      AggregatorFactory[] aggregators = config.getSchema().getDataSchema().getAggregators();
      uniqAggregators = new ArrayList<>();
      for (AggregatorFactory aggFactory : aggregators) {
        if (aggFactory.getTypeName().equals("unique")) {
          uniqAggregators.add(aggFactory);
        }
      }
    }

    @Override
    protected void innerMap(
        InputRow inputRow,
        Mapper.Context context
    ) throws IOException, InterruptedException
    {
      // Group by bucket, sort by timestamp
      final Optional<Bucket> bucket = getConfig().getBucket(inputRow);

      if (!bucket.isPresent()) {
        throw new ISE("WTF?! No bucket found for row: %s", inputRow);
      }

      for (int index = 0; index < uniqAggregators.size(); index++) {
        AggregatorFactory aggFactory = uniqAggregators.get(index);
        try {
          Method method = aggFactory.getClass().getMethod("getFieldName");
          String fieldName = (String) method.invoke(aggFactory);
          if (fieldName == null || fieldName.isEmpty()) {
            throw new RuntimeException("fieldName is null or empty");
          }

          Object raw = inputRow.getRaw(fieldName);

          List<String> values = Rows.objectToStrings(raw);
          for (String value : values) {
            writeValue(value, index, context);
          }

        }
        catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
          log.warn("broken unique agg:%s", aggFactory);
          throw new RuntimeException(e);
        }

      }

    }

    private void writeValue(String value, int index, Mapper.Context context) throws IOException, InterruptedException
    {
      byte[] bytesvalue = value.getBytes(Charset.forName("UTF-8"));

      tmpBuf.clear();
      int size = bytesvalue.length + 1;
      if (size >= tmpBuf.capacity()) {
        tmpBuf = ByteBuffer.allocate(countNewSize(tmpBuf.capacity(), size));
      }
      tmpBuf.put(toBytes(index)[3]);
      tmpBuf.put(bytesvalue, 0, bytesvalue.length);

      outputKey.set(tmpBuf.array(), 0, tmpBuf.position());
      sortableKey.init(outputKey);
      context.write(sortableKey, NullWritable.get());
    }

    private int countNewSize(int oldSize, int dataSize)
    {
      int newSize = oldSize * 2;
      while (newSize < dataSize) {
        newSize = newSize * 2;
      }
      return newSize;
    }

    /**
     * Convert an int value to a byte array.  Big-endian.  Same as what DataOutputStream.writeInt
     * does.
     *
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(int val)
    {
      byte[] b = new byte[4];
      for (int i = 3; i > 0; i--) {
        b[i] = (byte) val;
        val >>>= 8;
      }
      b[0] = (byte) val;
      return b;
    }

  }

  public static class BuildDictCombiner extends Reducer<SelfDefineSortableKey, NullWritable, SelfDefineSortableKey, NullWritable>
  {
    @Override
    protected void reduce(SelfDefineSortableKey skey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
      context.write(skey, NullWritable.get());
    }
  }

  public static class BuildDictReducer extends Reducer<SelfDefineSortableKey, NullWritable, NullWritable, NullWritable>
  {
    private static final Logger log = new Logger(BuildDictReducer.class);

    protected HadoopDruidIndexerConfig config;
    private AggregatorFactory uniqAggregator;
    private String fieldName;

    private GlobalDictionaryBuilder builder;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());

      AggregatorFactory[] aggregators = config.getSchema().getDataSchema().getAggregators();
      List<AggregatorFactory> uniqAggregators = new ArrayList<>();
      for (AggregatorFactory aggFactory : aggregators) {
        if (aggFactory.getTypeName().equals("unique")) {
          uniqAggregators.add(aggFactory);
        }
      }
      int taskId = context.getTaskAttemptID().getTaskID().getId();
      uniqAggregator = uniqAggregators.get(taskId);

      try {
        Method method = uniqAggregator.getClass().getMethod("getFieldName");
        fieldName = (String) method.invoke(uniqAggregator);

        if (fieldName == null || fieldName.isEmpty()) {
          throw new RuntimeException("fieldName is null or empty");
        }

        log.info("column name: " + fieldName);

      }
      catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        log.warn("broken unique agg:%s", uniqAggregator);
        throw new RuntimeException(e);
      }

      HadoopUtil.setCurrentConfiguration(context.getConfiguration());

      Properties prop = new Properties();
      prop.setProperty("kylin.hdfs.working.dir", config.getWorkingPath());

      KylinConfig.setKylinConfigInEnvIfMissing(prop);

      this.builder = new GlobalDictionaryBuilder();
      builder.init(config.getSchema().getDataSchema().getDataSource(), fieldName, KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(),
                   context.getConfiguration().get(HadoopDruidIndexerConfig.CONFIG_PROPERTY + ".zkhosts"),
                   context.getConfiguration().get(HadoopDruidIndexerConfig.CONFIG_PROPERTY + ".zkbase"));

    }

    @Override
    protected void reduce(SelfDefineSortableKey skey, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException
    {
      Text key = skey.getText();
      String value = toString(key.getBytes(), 1, key.getLength() - 1);
      builder.addValue(value);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException
    {
      AppendTrieDictionary<String> dict = builder.build();

      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (PrintStream ps = new PrintStream(baos, true, "UTF-8")) {
        dict.dump(ps);
      }
      String dictInfo = new String(baos.toByteArray(), StandardCharsets.UTF_8);

      log.info("dict info:%s,min:%s,max:%s", dictInfo, dict.getMinId(), dict.getMaxId() & 0xFFFFFFFFL);
    }


    public static String toString(final byte[] b, int off, int len)
    {
      if (b == null) {
        return null;
      }
      if (len == 0) {
        return "";
      }
      return new String(b, off, len, Charset.forName("UTF-8"));
    }
  }

  public static class BuildDictPartitioner extends Partitioner<SelfDefineSortableKey, NullWritable>
  {
    @Override
    public int getPartition(SelfDefineSortableKey skey, NullWritable value, int numReduceTasks)
    {
      return readUnsigned(skey.getText().getBytes(), 0, 1);
    }

    public static int readUnsigned(byte[] bytes, int offset, int size)
    {
      int integer = 0;
      for (int i = offset, n = offset + size; i < n; i++) {
        integer <<= 8;
        integer |= (int) bytes[i] & 0xFF;
      }
      return integer;
    }
  }

}
