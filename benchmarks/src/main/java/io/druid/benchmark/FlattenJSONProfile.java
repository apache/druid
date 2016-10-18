/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.benchmark;

//import com.yourkit.api.Controller;

/**
 * Test app for profiling JSON parsing behavior. Uses the proprietary YourKit API, so this file
 * is commented out to prevent dependency resolution issues. Kept here for future usage/reference.
 */
public class FlattenJSONProfile
{

  /*
  private static final int numEvents = 400000;

  List<String> flatInputs;
  List<String> nestedInputs;
  Parser flatParser;
  Parser nestedParser;
  Parser fieldDiscoveryParser;
  Parser forcedPathParser;
  int flatCounter = 0;
  int nestedCounter = 0;

  public void prepare() throws Exception
  {
    FlattenJSONBenchmarkUtil gen = new FlattenJSONBenchmarkUtil();
    flatInputs = new ArrayList<String>();
    for (int i = 0; i < numEvents; i++) {
      flatInputs.add(gen.generateFlatEvent());
    }
    nestedInputs = new ArrayList<String>();
    for (int i = 0; i < numEvents; i++) {
      nestedInputs.add(gen.generateNestedEvent());
    }

    flatParser = gen.getFlatParser();
    nestedParser = gen.getNestedParser();
    fieldDiscoveryParser = gen.getFieldDiscoveryParser();
    forcedPathParser = gen.getForcedPathParser();
  }

  public Map<String, Object> parseNested(Parser parser)
  {
    Map<String, Object> parsed = parser.parse(nestedInputs.get(nestedCounter));
    nestedCounter = (nestedCounter + 1) % numEvents;
    return parsed;
  }

  public Map<String, Object> parseFlat(Parser parser)
  {
    Map<String, Object> parsed = parser.parse(flatInputs.get(flatCounter));
    flatCounter = (flatCounter + 1) % numEvents;
    return parsed;
  }


  public static void main(String[] args) throws Exception
  {
    FlattenJSONProfile fjp = new FlattenJSONProfile();
    fjp.prepare();
    Map<String, Object> parsedMap;
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    JSONPathSpec flattenSpec = new JSONPathSpec(true, fields);
    JSONParseSpec parseSpec = new JSONParseSpec(
        new TimestampSpec("ts", "iso", null),
        new DimensionsSpec(null, null, null),
        flattenSpec
    );
    Parser parser = fjp.fieldDiscoveryParser;
    Parser nestedPar = fjp.nestedParser;
    Parser forcedParser = fjp.forcedPathParser;

    int j = 0;
    Controller control = new Controller();
    control.stopCPUProfiling();
    control.clearCPUData();
    Thread.sleep(5000);
    control.startCPUSampling(null);
    for(int i = 0; i < numEvents; i++) {
      //parsedMap = parser.parse(fjp.nestedInputs.get(i));
      parsedMap = fjp.parseFlat(forcedParser);
      //parsedMap = fjp.parseFlat(parser);
      //parsedMap = fjp.parseNested(nestedPar);
      if(parsedMap != null) {
        j++;
      }
    }
    control.stopCPUProfiling();
    System.out.println(j);
  }
*/

}
