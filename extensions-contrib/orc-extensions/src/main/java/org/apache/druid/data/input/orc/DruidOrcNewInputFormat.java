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

package org.apache.druid.data.input.orc;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidOrcNewInputFormat extends InputFormat<NullWritable, Map<String, Object>>
{
  private final OrcNewInputFormat orcNewInputFormat;

  public DruidOrcNewInputFormat()
  {
    orcNewInputFormat = new OrcNewInputFormat();
  }

  @Override
  public RecordReader<NullWritable, Map<String, Object>> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
  {
    FileSplit fileSplit = (FileSplit) split;
    Path path = fileSplit.getPath();
    Configuration conf = ShimLoader.getHadoopShims().getConfiguration(context);
    Reader file = OrcFile.createReader(path, OrcFile.readerOptions(conf));
    StructObjectInspector inspector = (StructObjectInspector) file.getObjectInspector();
    RecordReader<NullWritable, OrcStruct> reader = orcNewInputFormat.createRecordReader(split, context);
    return new DruidOrcRecordReader(inspector, reader);
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException
  {
    return orcNewInputFormat.getSplits(context);
  }

  private static class DruidOrcRecordReader extends RecordReader<NullWritable, Map<String, Object>>
  {
    private final StructObjectInspector inspector;
    private final RecordReader<NullWritable, OrcStruct> orcRecordReader;

    private DruidOrcRecordReader(StructObjectInspector inspector, RecordReader<NullWritable, OrcStruct> orcRecordReader)
    {
      this.inspector = inspector;
      this.orcRecordReader = orcRecordReader;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException
    {
      orcRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException
    {
      return orcRecordReader.nextKeyValue();
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException
    {
      return orcRecordReader.getCurrentKey();
    }

    @Override
    public Map<String, Object> getCurrentValue() throws IOException, InterruptedException
    {
      OrcStruct value = orcRecordReader.getCurrentValue();

      Map<String, Object> map = new HashMap<>();
      List<? extends StructField> fields = inspector.getAllStructFieldRefs();
      for (StructField field : fields) {
        ObjectInspector objectInspector = field.getFieldObjectInspector();
        switch (objectInspector.getCategory()) {
          case PRIMITIVE:
            PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) objectInspector;
            map.put(
                field.getFieldName(),
                coercePrimitiveObject(
                    primitiveObjectInspector.getPrimitiveJavaObject(inspector.getStructFieldData(value, field))
                )
            );
            break;
          case LIST:  // array case - only 1-depth array supported yet
            ListObjectInspector listObjectInspector = (ListObjectInspector) objectInspector;
            map.put(
                field.getFieldName(),
                getListObject(listObjectInspector, inspector.getStructFieldData(value, field))
            );
            break;
          case MAP:
            MapObjectInspector mapObjectInspector = (MapObjectInspector) objectInspector;
            addMapValues(map, field.getFieldName(), mapObjectInspector, inspector.getStructFieldData(value, field));
            break;
          default:
            break;
        }
      }
      
      return map;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException
    {
      return orcRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException
    {
      orcRecordReader.close();
    }



    private static Object coercePrimitiveObject(final Object object)
    {
      if (object instanceof HiveDecimal) {
        return ((HiveDecimal) object).doubleValue();
      } else {
        return object;
      }
    }

    private List<Object> getListObject(ListObjectInspector listObjectInspector, Object listObject)
    {
      if (listObjectInspector.getListLength(listObject) < 0) {
        return null;
      }
      List<?> objectList = listObjectInspector.getList(listObject);
      List<Object> list = null;
      ObjectInspector child = listObjectInspector.getListElementObjectInspector();
      switch (child.getCategory()) {
        case PRIMITIVE:
          final PrimitiveObjectInspector primitiveObjectInspector = (PrimitiveObjectInspector) child;
          list = Lists.transform(objectList, new Function()
            {
              @Nullable
              @Override
              public Object apply(@Nullable Object input)
              {
                return coercePrimitiveObject(primitiveObjectInspector.getPrimitiveJavaObject(input));
              }
            }
          );
          break;
        default:
          break;
      }

      return list;
    }

    private void addMapValues(Map<String, Object> parsedMap, String fieldName, MapObjectInspector mapObjectInspector, Object mapObject)
    {
      if (mapObjectInspector.getMapSize(mapObject) < 0) {
        return;
      }
      ObjectInspector keyoip = mapObjectInspector.getMapKeyObjectInspector();
      ObjectInspector valueoip = mapObjectInspector.getMapValueObjectInspector();
      if (keyoip.getCategory() != ObjectInspector.Category.PRIMITIVE || valueoip.getCategory() != ObjectInspector.Category.PRIMITIVE) {
        return;
      }

      PrimitiveObjectInspector keyInspector = (PrimitiveObjectInspector) keyoip;
      PrimitiveObjectInspector valueInspector = (PrimitiveObjectInspector) valueoip;

      Map objectMap = mapObjectInspector.getMap(mapObject);

      objectMap.forEach((k, v) -> {
        String resolvedFieldName = fieldName + "_" + keyInspector.getPrimitiveJavaObject(k);
        parsedMap.put(resolvedFieldName, valueInspector.getPrimitiveJavaObject(v));
      });
    }
  }
}
