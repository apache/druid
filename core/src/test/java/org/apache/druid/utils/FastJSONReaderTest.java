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

package org.apache.druid.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FastJSONReaderTest
{
  @Test
  public void testGet()
  {
    String space = "  {  \"optOut\"  :  \"0\"  ,  \"page\"  :  \"\"  ,  \"mode\"  :  \"light\"  ,  \"screen\"  :  \"\"  }  ";
    evalString(space, "light", "mode");
    evalString(space, "0", "optOut");
    evalString(space, "", "screen");
    evalString(space, null, "not_exist");

    String spaceComplex = "   {  \"ary\"  :  [  1   ,  2  ]  ,  \"obj\"  :  {  \"nobj\"  :  {  \"k1\"  :  \"v1\"  ,  \"k2\"  : 123}  }  ";
    evalString(spaceComplex, "v1", "obj", "nobj", "k1");
    evalString(spaceComplex, "123", "obj", "nobj", "k2");
    evalString(spaceComplex, "[  1   ,  2  ]", "ary");
    evalString(spaceComplex, "{  \"k1\"  :  \"v1\"  ,  \"k2\"  : 123}", "obj", "nobj");
    evalString(spaceComplex, "{  \"nobj\"  :  {  \"k1\"  :  \"v1\"  ,  \"k2\"  : 123}  }", "obj");

    evalString("{\"optOut\":\"0\",\"page\":\"\",\"mode\":\"light\",\"screen\":\"\"}",
        "light", "mode");
    evalString("{\"ab_unit_n\":\"ABC-uref1023@32jds\",\"screen\":\"알림\",\"page\":\"알림\",\"mode\":\"dark\"}",
        "dark", "mode");
    // Invalid JSON, unclosed string
    evalString("{\"grpid\":\"17XX8\"," +
            "\"grpcode\":\"kimyoungrip\"," +
            "\"grpname\":\"옥스포드맨\"," +
            "\"fldid\":\"HBZs\"," +
            "\"dataid\":\"422\"," +
            "\"query\":\",울트라맨 베티저블 카카로트28,\\\"}\n\n",
        null, "반응유형");
    evalString("{\"ab_unit_n\":\"ABC-uref1023@32jds\",\"screen\":\"알림\",\"page\":\"알림\",\"mode\":\"dark\"}",
        null, "not_exist");

    String duplicatedNestedKey = "{\"a\":{\"bb\":123,\"cc\":\"val\",\"na\":[1,2,3,4,5]}," +
        "\"b\":{\"bb\":999}," +
        "\"bb\":123," +
        "\"custom_props\":{\"key\":\"value\",\"haha\":\"hoho\"}," +
        "\"lastkey\":\"lastval\"}";
    evalString(duplicatedNestedKey, "123", "a", "bb");
    evalString(duplicatedNestedKey, "999", "b", "bb");
    evalString(duplicatedNestedKey, "123", "bb");
    evalString(duplicatedNestedKey, "hoho", "custom_props", "haha");

    String nested3 = "{\"a\":{\"b\":{\"c\":\"first_c\"}},\"k\":{\"b\":{\"c\":\"second_c\"}}}";
    evalString(nested3, "first_c", "a", "b", "c");
    evalString(nested3, "second_c", "k", "b", "c");

    evalString("{\"a\":{\"bb\":123,\"cc\":\"val\",\"na\":[1,2,3,4,5]}}",
        "{\"bb\":123,\"cc\":\"val\",\"na\":[1,2,3,4,5]}", "a");
    evalString("{\"a\":[\"abc\",123,{\"bb\":123,\"cc\":\"val\",\"na\":[1,2,3,4,5]),123.456]}",
        "[\"abc\",123,{\"bb\":123,\"cc\":\"val\",\"na\":[1,2,3,4,5]),123.456]", "a");
    evalString("{\"a\":[\"abc\",123,123.456]}",
        "[\"abc\",123,123.456]", "a");
    evalString("{\"test_file_hash_key\":\"231dsfgrqwds\"," +
            "\"test_user_id_type\":\"app_user_id\"," +
            "\"mode\":\"pager\"," +
            "\"test_action_type\":\"imp\"," +
            "\"single_id\":\"143252341\"," +
            "\"test_event_meta_id\":\"324111423\"," +
            "\"bm\":\"T\"," +
            "\"type\":\"TimeFree_Series\"," +
            "\"category\":\"Novel_Fantasy\"," +
            "\"value\":," +
            "\"series_id\":\"324111423\"}",
        null, "value");
    evalString("{\"test_file_hash_key\":\"231dsfgrqwds\"," +
            "\"test_user_id_type\":," +
            "\"mode\":\"pager\"," +
            "\"test_action_type\":\"imp\"," +
            "\"single_id\":\"143252341\"," +
            "\"test_event_meta_id\":\"324111423\"," +
            "\"bm\":\"T\"," +
            "\"type\":\"TimeFree_Series\"," +
            "\"category\":\"Novel_Fantasy\"," +
            "\"value\":," +
            "\"series_id\":\"324111423\"}",
        null, "value");
    evalString("{\"queries\":{\"plugin_id\":\"132rufehfwhh30211\"," +
            "\"abctouch_id\":\"ABC_321G_DFS_12341231_FDS_DSA_2111-01-24T10:35:24.931617000\"," +
            "\"trace_id\":\"null\"," +
            "\"profile\":\"Nkvwxb\"," +
            "\"software_type\":\"ONE_TIME_INSTALLMENT\"," +
            "\"app_user_id\":\"445955333\"," +
            "\"bot_api_token\":\"abc-8a4f3bb69320484b9875669ecfa57cf3\"," +
            "\"transaction_id\":\"12ewds-12f-231eqwdfs-231ewrf\")," +
            "\"profile\":\"hahahoho\"}",
        "hahahoho", "profile");
    evalString("{\"queries\":{\"plugin_id\":\"132rufehfwhh30211\"," +
            "\"abctouch_id\":\"ABC_321G_DFS_12341231_FDS_DSA_2111-01-24T10:35:24.931617000\"," +
            "\"trace_id\":\"null\"," +
            "\"profile\":\"Nkvwxb\"," +
            "\"software_type\":\"ONE_TIME_INSTALLMENT\"," +
            "\"app_user_id\":\"445955333\"," +
            "\"bot_api_token\":\"abc-8a4f3bb69320484b9875669ecfa57cf3\"," +
            "\"transaction_id\":\"12ewds-12f-231eqwdfs-231ewrf\"}}",
        null, "profile");
    evalString("{\"profile\":\"\",\"isDev\":\"false\"}",
        "", "profile");
    evalString("{\"profile\":\"PRODUCTION\",\"isDev\":false}",
        "PRODUCTION", "profile");
    evalString("{\"profile\":\"prod\",\"isDev\":\"false\"}",
        "prod", "profile");
    evalString("{\"profile\":\"real\",\"isDev\":\"false\"}",
        "real", "profile");
    evalString("{\"profile\":\"PRODUCTION\",\"isDev\":false}",
        "false", "isDev");
    evalString("{\"t\":\"m\",\" l\":\"n\",\" s\":\"off\"}",
        "n", " l");

    // Invalid String
    evalString("{\"a\":\"string}", null, "a");
    // Invalid number
    evalNumber("{\"a\":123.123.1}", null, "a");
    // Empty Array
    evalString("{\"a\":[]", "[]", "a");
  }

  private void evalString(String jsonStr, String expected, String... args)
  {
    FastJSONReader reader = new FastJSONReader(jsonStr);
    assertEquals(expected, reader.get(args));
  }

  private void evalNumber(String jsonStr, String expected, String... args)
  {
    FastJSONReader reader = new FastJSONReader(jsonStr);
    Double result = null;
    try {
      result = Double.valueOf(reader.get(args));
    }
    catch (Exception e) {
      // Ingore
    }
    assertEquals(expected, result);
  }
}
