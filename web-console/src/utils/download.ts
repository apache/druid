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
import { Align, getMarkdownTable } from 'markdown-table-ts';

import { copyAndAlert, stringifyValue } from './general';
import { queryResultToValuesQuery } from './values-query';

export type FileFormat = 'csv' | 'tsv' | 'json' | 'sql' | 'markdown';
export const FILE_FORMATS: FileFormat[] = ['csv', 'tsv', 'json', 'sql', 'markdown'];

const FILE_FORMAT_TO_MIME_TYPE: Record<FileFormat, string> = {
  csv: 'text/csv',
  tsv: 'text/tab-separated-values',
  json: 'application/json',
  sql: 'text/plain',
  markdown: 'text/markdown',
};

export const FILE_FORMAT_TO_LABEL: Record<FileFormat, string> = {
  csv: 'CSV',
  tsv: 'TSV',
  json: 'JSON (new line delimited)',
  sql: 'SQL (VALUES)',
  markdown: 'Markdown table',
};

export function formatForFileFormat(
  s: null | string | number | Date,
  format: 'csv' | 'tsv',
): string {
  if (s == null) return '';

  // stringify and remove line break
  const str = stringifyValue(s).replace(/\r\n|\r|\n/g, ' ');

  if (format === 'csv') {
    // csv: single quote => double quote, handle ','
    return `"${str.replace(/"/g, '""')}"`;
  } else {
    // tsv: single quote => double quote, \t => ''
    return str.replace(/\t/g, '').replace(/"/g, '""');
  }
}

export function downloadFile(text: string, fileFormat: FileFormat, filename: string): void {
  FileSaver.saveAs(
    new Blob([text], {
      type: FILE_FORMAT_TO_MIME_TYPE[fileFormat],
    }),
    filename,
  );
}

export function queryResultsToString(queryResult: QueryResult, format: FileFormat): string {
  const { header, rows } = queryResult;

  switch (format) {
    case 'csv':
    case 'tsv': {
      const separator = format === 'csv' ? ',' : '\t';
      return [
        header.map(column => formatForFileFormat(column.name, format)).join(separator),
        ...rows.map(row => row.map(cell => formatForFileFormat(cell, format)).join(separator)),
      ].join('\n');
    }

    case 'sql':
      return queryResultToValuesQuery(queryResult).toString();

    case 'json':
      return queryResult
        .toObjectArray()
        .map(r => JSONBig.stringify(r))
        .join('\n');

    case 'markdown':
      return getMarkdownTable({
        table: {
          head: header.map(column => column.name),
          body: rows.map(row => row.map(cell => stringifyValue(cell))),
        },
        alignment: header.map(column => (column.isNumeric() ? Align.Right : Align.Left)),
      });

    default:
      throw new Error(`unknown format: ${format}`);
  }
}

export function downloadQueryResults(
  queryResult: QueryResult,
  filename: string,
  fileFormat: FileFormat,
): void {
  const resultString: string = queryResultsToString(queryResult, fileFormat);
  downloadFile(resultString, fileFormat, filename);
}

export function copyQueryResultsToClipboard(
  queryResult: QueryResult,
  fileFormat: FileFormat,
): void {
  const resultString: string = queryResultsToString(queryResult, fileFormat);
  copyAndAlert(resultString, 'Query results copied to clipboard');
}
