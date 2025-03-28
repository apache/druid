/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { QueryResult, sane } from 'druid-query-toolkit';

import { queryResultsToString, stringifyCsvValue, stringifyTsvValue } from './download';

describe('download', () => {
  it('stringifyCsvValue', () => {
    expect(stringifyCsvValue(null)).toEqual('');
    expect(stringifyCsvValue('')).toEqual('');
    expect(stringifyCsvValue('null')).toEqual('null');
    expect(stringifyCsvValue('hello\nworld')).toEqual('hello world');
    expect(stringifyCsvValue('Jane "JD" Smith')).toEqual('"Jane ""JD"" Smith"');
    expect(stringifyCsvValue(123)).toEqual('123');
    expect(stringifyCsvValue(new Date('2021-01-02T03:04:05.678Z'))).toEqual(
      '2021-01-02T03:04:05.678Z',
    );
  });

  it('stringifyTsvValue', () => {
    expect(stringifyTsvValue(null)).toEqual('');
    expect(stringifyTsvValue('')).toEqual('');
    expect(stringifyTsvValue('null')).toEqual('null');
    expect(stringifyTsvValue('hello\nworld')).toEqual('hello world');
    expect(stringifyTsvValue(123)).toEqual('123');
    expect(stringifyTsvValue(new Date('2021-01-02T03:04:05.678Z'))).toEqual(
      '2021-01-02T03:04:05.678Z',
    );
  });

  describe('queryResultsToString', () => {
    /*
    Data for testing:
    =================
    {"name": "John, Doe", "age": 29, "city": "New York", "bio": "Loves coding\nand coffee"}
    {"name": "Jane \"JD\" Smith", "age": 34, "city": "Los Angeles", "bio": "Enjoys \"live music\" and travel"}
    {"name": "Michael, O'Connor", "age": 41, "city": "Chicago", "bio": "Expert in AI\\ML"}
    {"name": "李四 (Li Si)", "age": 27, "city": "北京 (Beijing)", "bio": "喜欢编程和阅读"}
    {"name": "O'Reilly, Patrick", "age": 45, "city": null, "bio": "\nLoves\ttabs"}
     */

    const queryResult = QueryResult.fromRawResult(
      [
        ['__time', 'name', 'age', 'city', 'bio'],
        ['LONG', 'STRING', 'LONG', 'STRING', 'STRING'],
        ['TIMESTAMP', 'VARCHAR', 'BIGINT', 'VARCHAR', 'VARCHAR'],
        [
          '1970-01-01T00:00:00.000Z',
          'Jane "JD" Smith',
          34,
          'Los Angeles',
          'Enjoys "live music" and travel',
        ],
        ['1970-01-01T00:00:00.000Z', 'John, Doe', 29, 'New York', 'Loves coding\nand coffee'],
        ['1970-01-01T00:00:00.000Z', "Michael, O'Connor", 41, 'Chicago', 'Expert in AI\\ML'],
        ['1970-01-01T00:00:00.000Z', "O'Reilly, Patrick", 45, null, '\nLoves\ttabs'],
        ['1970-01-01T00:00:00.000Z', '李四 (Li Si)', 27, '北京 (Beijing)', '喜欢编程和阅读'],
      ],
      true,
      true,
      true,
      true,
    );

    it('works with CSV', () => {
      expect(queryResultsToString(queryResult, 'csv').replaceAll('\t', '\\t')).toEqual(sane`
        __time,name,age,city,bio
        1970-01-01T00:00:00.000Z,"Jane ""JD"" Smith",34,Los Angeles,"Enjoys ""live music"" and travel"
        1970-01-01T00:00:00.000Z,"John, Doe",29,New York,Loves coding and coffee
        1970-01-01T00:00:00.000Z,"Michael, O'Connor",41,Chicago,Expert in AI\\ML
        1970-01-01T00:00:00.000Z,"O'Reilly, Patrick",45,," Loves\ttabs"
        1970-01-01T00:00:00.000Z,李四 (Li Si),27,北京 (Beijing),喜欢编程和阅读
      `);
    });

    it('works with TSV', () => {
      expect(queryResultsToString(queryResult, 'tsv').replaceAll('\t', '\\t')).toEqual(sane`
        __time\tname\tage\tcity\tbio
        1970-01-01T00:00:00.000Z\tJane "JD" Smith\t34\tLos Angeles\tEnjoys "live music" and travel
        1970-01-01T00:00:00.000Z\tJohn, Doe\t29\tNew York\tLoves coding and coffee
        1970-01-01T00:00:00.000Z\tMichael, O'Connor\t41\tChicago\tExpert in AI\\ML
        1970-01-01T00:00:00.000Z\tO'Reilly, Patrick\t45\t\t Loves tabs
        1970-01-01T00:00:00.000Z\t李四 (Li Si)\t27\t北京 (Beijing)\t喜欢编程和阅读
      `);
    });

    it('works with JSON', () => {
      expect(queryResultsToString(queryResult, 'json')).toEqual(sane`
        {"__time":"1970-01-01T00:00:00.000Z","name":"Jane \\"JD\\" Smith","age":34,"city":"Los Angeles","bio":"Enjoys \\"live music\\" and travel"}
        {"__time":"1970-01-01T00:00:00.000Z","name":"John, Doe","age":29,"city":"New York","bio":"Loves coding\\nand coffee"}
        {"__time":"1970-01-01T00:00:00.000Z","name":"Michael, O'Connor","age":41,"city":"Chicago","bio":"Expert in AI\\\\ML"}
        {"__time":"1970-01-01T00:00:00.000Z","name":"O'Reilly, Patrick","age":45,"city":null,"bio":"\\nLoves\\ttabs"}
        {"__time":"1970-01-01T00:00:00.000Z","name":"李四 (Li Si)","age":27,"city":"北京 (Beijing)","bio":"喜欢编程和阅读"}
      `);
    });

    it('works with SQL', () => {
      expect(queryResultsToString(queryResult, 'sql')).toEqual(sane`
        SELECT
          CAST("c1" AS TIMESTAMP) AS "__time",
          CAST("c2" AS VARCHAR) AS "name",
          CAST("c3" AS BIGINT) AS "age",
          CAST("c4" AS VARCHAR) AS "city",
          CAST("c5" AS VARCHAR) AS "bio"
        FROM (
          VALUES
          ('1970-01-01T00:00:00.000Z', 'Jane "JD" Smith', 34, 'Los Angeles', 'Enjoys "live music" and travel'),
          ('1970-01-01T00:00:00.000Z', 'John, Doe', 29, 'New York', U&'Loves coding\000aand coffee'),
          ('1970-01-01T00:00:00.000Z', 'Michael, O''Connor', 41, 'Chicago', 'Expert in AI\ML'),
          ('1970-01-01T00:00:00.000Z', 'O''Reilly, Patrick', 45, NULL, U&'\000aLoves\0009tabs'),
          ('1970-01-01T00:00:00.000Z', '李四 (Li Si)', 27, '北京 (Beijing)', '喜欢编程和阅读')
        ) AS "t" ("c1", "c2", "c3", "c4", "c5")
      `);
    });

    it('works with Markdown', () => {
      expect(queryResultsToString(queryResult, 'markdown').replaceAll('\t', '\\t')).toEqual(sane`
        | __time                   | name              | age | city         | bio                            |
        | :----------------------- | :---------------- | --: | :----------- | :----------------------------- |
        | 1970-01-01T00:00:00.000Z | Jane "JD" Smith   |  34 | Los Angeles  | Enjoys "live music" and travel |
        | 1970-01-01T00:00:00.000Z | John, Doe         |  29 | New York     | Loves coding<br>and coffee     |
        | 1970-01-01T00:00:00.000Z | Michael, O'Connor |  41 | Chicago      | Expert in AI\\ML                |
        | 1970-01-01T00:00:00.000Z | O'Reilly, Patrick |  45 |              | <br>Loves\ttabs                 |
        | 1970-01-01T00:00:00.000Z | 李四 (Li Si)        |  27 | 北京 (Beijing) | 喜欢编程和阅读                        |
      `);
    });
  });
});
