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

import { QueryResult } from 'druid-query-toolkit';
import FileSaver from 'file-saver';
import * as JSONBig from 'json-bigint-native';

import { stringifyValue } from './general';

export function formatForFormat(s: null | string | number | Date, format: 'csv' | 'tsv'): string {
  // stringify and remove line break
  const str = stringifyValue(s).replace(/(?:\r\n|\r|\n)/g, ' ');

  if (format === 'csv') {
    // csv: single quote => double quote, handle ','
    return `"${str.replace(/"/g, '""')}"`;
  } else {
    // tsv: single quote => double quote, \t => ''
    return str.replace(/\t/g, '').replace(/"/g, '""');
  }
}

export function downloadFile(text: string, type: string, filename: string): void {
  let blobType;
  switch (type) {
    case 'json':
      blobType = 'application/json';
      break;
    case 'tsv':
      blobType = 'text/tab-separated-values';
      break;
    default:
      // csv
      blobType = `text/${type}`;
      break;
  }

  const blob = new Blob([text], {
    type: blobType,
  });

  FileSaver.saveAs(blob, filename);
}

export function downloadQueryResults(
  queryResult: QueryResult,
  filename: string,
  format: string,
): void {
  let lines: string[] = [];
  let separator = '';

  if (format === 'csv' || format === 'tsv') {
    separator = format === 'csv' ? ',' : '\t';
    lines.push(
      queryResult.header.map(column => formatForFormat(column.name, format)).join(separator),
    );
    lines = lines.concat(
      queryResult.rows.map(r => r.map(cell => formatForFormat(cell, format)).join(separator)),
    );
  } else {
    // json
    lines = queryResult.rows.map(r => {
      const outputObject: Record<string, any> = {};
      for (let k = 0; k < r.length; k++) {
        const newName = queryResult.header[k];
        if (newName) {
          outputObject[newName.name] = r[k];
        }
      }
      return JSONBig.stringify(outputObject);
    });
  }

  const lineBreak = '\n';
  downloadFile(lines.join(lineBreak), format, filename);
}
