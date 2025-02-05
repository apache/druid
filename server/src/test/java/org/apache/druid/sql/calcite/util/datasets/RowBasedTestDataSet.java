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

package org.apache.druid.sql.calcite.util.datasets;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.InputSourceReader;
import org.apache.druid.indexing.input.InputRowSchemas;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.indexing.DataSchema;

import java.io.File;
import java.io.IOException;
import java.util.List;

public abstract class RowBasedTestDataSet extends AbstractRowBasedTestDataset
{
  private TestDataSet buildTestDataset(
      DataSchema dataSchema,
      InputFormat inputFormat,
      InputSource inputSource,
      File tempDir)
      throws IOException
  {

    return new RowBasedTestDataSet() {

      @Override
      public Iterable<InputRow> getRows()
      {
        if(true)
        {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;
        
      }

      @Override
      public InputRowSchema getInputRowSchema()
      {
        if(true)
        {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;
        
      }

      @Override
      public List<AggregatorFactory> getMetrics()
      {
        if(true)
        {
          throw new RuntimeException("FIXME: Unimplemented!");
        }
        return null;
        
      }

    }

    InputRowSchema inputRowSchema = InputRowSchemas.fromDataSchema(dataSchema);
    InputSourceReader reader = inputSource.reader(inputRowSchema, inputFormat, tempDir);
    try (CloseableIterator<InputRow> it = reader.read()) {
      while (it.hasNext()) {
        InputRow a = it.next();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

}
