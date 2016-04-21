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

package io.druid.hive;

import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import io.druid.indexer.hadoop.MapWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.lazy.LazySerDeParameters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;
import org.joda.time.DateTime;

import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 */
public class HiveDruidSerDe extends AbstractSerDe
{
  private static final Logger logger = new Logger(HiveDruidSerDe.class);

  private String[] columns;
  private ObjectInspector inspector;

  private int timeIndex = -1;
  private PrimitiveObjectInspector.PrimitiveCategory timeConvert;

  @Override
  public void initialize(Configuration configuration, Properties properties) throws SerDeException
  {
    LazySerDeParameters serdeParams = new LazySerDeParameters(configuration, properties, getClass().getName());

    List<String> columnNames = serdeParams.getColumnNames();
    List<TypeInfo> columnTypes = serdeParams.getColumnTypes();

    List<ObjectInspector> inspectors = Lists.newArrayListWithExpectedSize(columnNames.size());
    for (int i = 0; i < columnTypes.size(); ++i) {
      PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) columnTypes.get(i);
      inspectors.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo));
      if (typeInfo.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING
          && ExpressionConverter.TIME_COLUMN_NAME.equals(columnNames.get(i))) {
        timeConvert = typeInfo.getPrimitiveCategory();
        timeIndex = i;
      }
    }
    if (timeConvert != null &&
        timeConvert != PrimitiveObjectInspector.PrimitiveCategory.LONG &&
        timeConvert != PrimitiveObjectInspector.PrimitiveCategory.DATE &&
        timeConvert != PrimitiveObjectInspector.PrimitiveCategory.TIMESTAMP) {
      logger.warn("Not supported time conversion type " + timeConvert + ".. regarding to string");
      inspectors.set(timeIndex, PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      timeIndex = -1;
    }
    inspector = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, inspectors);
    columns = columnNames.toArray(new String[columnNames.size()]);
  }

  @Override
  public Class<? extends Writable> getSerializedClass()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public Writable serialize(Object o, ObjectInspector objectInspector) throws SerDeException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public SerDeStats getSerDeStats()
  {
    return new SerDeStats();
  }

  @Override
  public Object deserialize(Writable writable) throws SerDeException
  {
    MapWritable input = (MapWritable) writable;
    List output = Lists.newArrayListWithExpectedSize(columns.length);
    for (int i = 0; i < columns.length; i++) {
      Object v = input.getValue().get(columns[i]);
      if (v != null && i == timeIndex) {
        long timeMillis = new DateTime(v).getMillis();
        switch (timeConvert) {
          case LONG:
            v = timeMillis;
            break;
          case DATE:
            v = new Date(timeMillis);
            break;
          case TIMESTAMP:
            v = new Timestamp(timeMillis);
            break;
        }
      }
      output.add(v);
    }
    return output;
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException
  {
    return inspector;
  }
}
