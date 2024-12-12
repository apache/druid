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

import type { QueryResult } from 'druid-query-toolkit';
import FileSaver from 'file-saver';
import * as JSONBig from 'json-bigint-native';

import { copyAndAlert, stringifyValue } from './general';
import { queryResultToValuesQuery } from './values-query';

export type Format = 'csv' | 'tsv' | 'json' | 'sql';

export function downloadUrl(url: string, filename: string) {
  // Create a link and set the URL using `createObjectURL`
  const link = document.createElement('a');
  link.style.display = 'none';
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();

  // To make this work on Firefox we need to wait
  // a little while before removing it.
  setTimeout(() => {
    if (!link.parentNode) return;
    link.parentNode.removeChild(link);
  }, 0);
}

export function formatForFormat(s: null | string | number | Date, format: 'csv' | 'tsv'): string {
  if (s == null) return '';

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

function queryResultsToString(queryResult: QueryResult, format: Format): string {
  const { header, rows } = queryResult;

  switch (format) {
    case 'csv':
    case 'tsv': {
      const separator = format === 'csv' ? ',' : '\t';
      return [
        header.map(column => formatForFormat(column.name, format)).join(separator),
        ...rows.map(r => r.map(cell => formatForFormat(cell, format)).join(separator)),
      ].join('\n');
    }

    case 'sql':
      return queryResultToValuesQuery(queryResult).toString();

    case 'json':
      return queryResult
        .toObjectArray()
        .map(r => JSONBig.stringify(r))
        .join('\n');

    default:
      throw new Error(`unknown format: ${format}`);
  }
}

export function downloadQueryResults(
  queryResult: QueryResult,
  filename: string,
  format: Format,
): void {
  const resultString: string = queryResultsToString(queryResult, format);
  downloadFile(resultString, format, filename);
}

export function copyQueryResultsToClipboard(queryResult: QueryResult, format: Format): void {
  const resultString: string = queryResultsToString(queryResult, format);
  copyAndAlert(resultString, 'Query results copied to clipboard');
}
