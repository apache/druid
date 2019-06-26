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

package org.apache.druid.data.input.avro;

import org.apache.druid.data.input.AvroStreamInputRowParserTest;
import org.apache.druid.data.input.SomeAvroDatum;
import org.junit.Assert;
import org.junit.Test;

public class AvroFlattenerMakerTest
{

  @Test
  public void getRootField()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false);

    Assert.assertEquals(
        record.timestamp,
        flattener.getRootField(record, "timestamp")
    );
    Assert.assertEquals(
        record.eventType,
        flattener.getRootField(record, "eventType")
    );
    Assert.assertEquals(
        record.id,
        flattener.getRootField(record, "id")
    );
    Assert.assertEquals(
        record.someOtherId,
        flattener.getRootField(record, "someOtherId")
    );
    Assert.assertEquals(
        record.isValid,
        flattener.getRootField(record, "isValid")
    );
    Assert.assertEquals(
        record.someIntArray,
        flattener.getRootField(record, "someIntArray")
    );
    Assert.assertEquals(
        record.someStringArray,
        flattener.getRootField(record, "someStringArray")
    );
    Assert.assertEquals(
        record.someIntValueMap,
        flattener.getRootField(record, "someIntValueMap")
    );
    Assert.assertEquals(
        record.someStringValueMap,
        flattener.getRootField(record, "someStringValueMap")
    );
    Assert.assertEquals(
        record.someUnion,
        flattener.getRootField(record, "someUnion")
    );
    Assert.assertEquals(
        record.someNull,
        flattener.getRootField(record, "someNull")
    );
    Assert.assertEquals(
        record.someFixed,
        flattener.getRootField(record, "someFixed")
    );
    Assert.assertEquals(
        // Casted to an array by transformValue
        record.someBytes.array(),
        flattener.getRootField(record, "someBytes")
    );
    Assert.assertEquals(
        record.someEnum,
        flattener.getRootField(record, "someEnum")
    );
    Assert.assertEquals(
        record.someRecord,
        flattener.getRootField(record, "someRecord")
    );
    Assert.assertEquals(
        record.someLong,
        flattener.getRootField(record, "someLong")
    );
    Assert.assertEquals(
        record.someInt,
        flattener.getRootField(record, "someInt")
    );
    Assert.assertEquals(
        record.someFloat,
        flattener.getRootField(record, "someFloat")
    );
  }

  @Test
  public void makeJsonPathExtractor()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false);

    Assert.assertEquals(
        record.timestamp,
        flattener.makeJsonPathExtractor("$.timestamp").apply(record)
    );
    Assert.assertEquals(
        record.eventType,
        flattener.makeJsonPathExtractor("$.eventType").apply(record)
    );
    Assert.assertEquals(
        record.id,
        flattener.makeJsonPathExtractor("$.id").apply(record)
    );
    Assert.assertEquals(
        record.someOtherId,
        flattener.makeJsonPathExtractor("$.someOtherId").apply(record)
    );
    Assert.assertEquals(
        record.isValid,
        flattener.makeJsonPathExtractor("$.isValid").apply(record)
    );
    Assert.assertEquals(
        record.someIntArray,
        flattener.makeJsonPathExtractor("$.someIntArray").apply(record)
    );
    Assert.assertEquals(
        record.someStringArray,
        flattener.makeJsonPathExtractor("$.someStringArray").apply(record)
    );
    Assert.assertEquals(
        record.someIntValueMap,
        flattener.makeJsonPathExtractor("$.someIntValueMap").apply(record)
    );
    Assert.assertEquals(
        record.someStringValueMap,
        flattener.makeJsonPathExtractor("$.someStringValueMap").apply(record)
    );
    Assert.assertEquals(
        record.someUnion,
        flattener.makeJsonPathExtractor("$.someUnion").apply(record)
    );
    Assert.assertEquals(
        record.someNull,
        flattener.makeJsonPathExtractor("$.someNull").apply(record)
    );
    Assert.assertEquals(
        record.someFixed,
        flattener.makeJsonPathExtractor("$.someFixed").apply(record)
    );
    Assert.assertEquals(
        // Casted to an array by transformValue
        record.someBytes.array(),
        flattener.makeJsonPathExtractor("$.someBytes").apply(record)
    );
    Assert.assertEquals(
        record.someEnum,
        flattener.makeJsonPathExtractor("$.someEnum").apply(record)
    );
    Assert.assertEquals(
        record.someRecord,
        flattener.makeJsonPathExtractor("$.someRecord").apply(record)
    );
    Assert.assertEquals(
        record.someLong,
        flattener.makeJsonPathExtractor("$.someLong").apply(record)
    );
    Assert.assertEquals(
        record.someInt,
        flattener.makeJsonPathExtractor("$.someInt").apply(record)
    );
    Assert.assertEquals(
        record.someFloat,
        flattener.makeJsonPathExtractor("$.someFloat").apply(record)
    );
  }

  @Test(expected = UnsupportedOperationException.class)
  public void makeJsonQueryExtractor()
  {
    final SomeAvroDatum record = AvroStreamInputRowParserTest.buildSomeAvroDatum();
    final AvroFlattenerMaker flattener = new AvroFlattenerMaker(false);

    Assert.assertEquals(
        record.timestamp,
        flattener.makeJsonQueryExtractor("$.timestamp").apply(record)
    );
  }
}
