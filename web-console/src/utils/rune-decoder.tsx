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

export interface HeaderRows {
  header: string[];
  rows: any[][];
}

const SUPPORTED_QUERY_TYPES: string[] = [
  'timeseries',
  'topN',
  'search',
  'groupBy',
  'timeBoundary',
  'dataSourceMetadata',
  'select',
  'scan',
  'segmentMetadata'
];

function flatArrayToHeaderRows(a: Record<string, any>[], headerOverride?: string[]): HeaderRows {
  if (!a.length) return { header: [], rows: [] };
  const header = headerOverride || Object.keys(a[0]);
  return {
    header,
    rows: a.map(r => header.map(h => r[h]))
  };
}

function processTimeseries(rune: any[]): HeaderRows {
  const header = Object.keys(rune[0].result);
  return {
    header: ['timestamp'].concat(header),
    rows: rune.map((r: Record<string, any>) => {
      const { timestamp, result } = r;
      return [timestamp].concat(header.map(h => result[h]));
    })
  };
}

function processArrayWithResultArray(rune: any[]): HeaderRows {
  return flatArrayToHeaderRows([].concat(...rune.map(r => r.result)));
}

function processArrayWithEvent(rune: any[]): HeaderRows {
  return flatArrayToHeaderRows(rune.map((r: any) => r.event));
}

function processArrayWithResult(rune: any[]): HeaderRows {
  return flatArrayToHeaderRows(rune.map((r: any) => r.result));
}

function processSelect(rune: any[]): HeaderRows {
  return flatArrayToHeaderRows([].concat(...rune.map(r => r.result.events.map((e: any) => e.event))));
}

function processScan(rune: any[]): HeaderRows {
  const header = rune[0].columns;
  return flatArrayToHeaderRows([].concat(...rune.map(r => r.events)), header);
}

function processSegmentMetadata(rune: any[]): HeaderRows {
  const flatArray = ([] as any).concat(...rune.map(r => Object.keys(r.columns).map(
    k => Object.assign({id: r.id, column: k},  r.columns[k])
  )));
  return flatArrayToHeaderRows(flatArray);
}

export function decodeRune(runeQuery: any, runeResult: any[]): HeaderRows {
  let queryType = runeQuery.queryType;
  if (typeof queryType !== 'string') throw new Error('must have queryType');
  if (!SUPPORTED_QUERY_TYPES.includes(queryType)) {
    const treatQueryTypeAs = (runeQuery.context || {}).treatQueryTypeAs;
    if (typeof treatQueryTypeAs === 'string') {
      if (SUPPORTED_QUERY_TYPES.includes(treatQueryTypeAs)) {
        queryType = treatQueryTypeAs;
      } else {
        throw new Error(`Unsupported query type in treatQueryTypeAs: '${treatQueryTypeAs}'`);
      }
    } else {
      throw new Error([
        `Unsupported query type '${queryType}'.`,
        `Supported query types are: '${SUPPORTED_QUERY_TYPES.join("', '")}'.`,
        `If this is a custom query you can parse the result as a known query type by setting 'treatQueryTypeAs' in the context to one of the supported types.`
      ].join(' '));
    }
  }

  if (!runeResult.length) return { header: [], rows: [] };

  switch (queryType) {
    case 'timeseries':
      return processTimeseries(runeResult);

    case 'topN':
    case 'search':
      return processArrayWithResultArray(runeResult);

    case 'groupBy':
      return processArrayWithEvent(runeResult);

    case 'timeBoundary':
    case 'dataSourceMetadata':
      return processArrayWithResult(runeResult);

    case 'select':
      return processSelect(runeResult);

    case 'scan':
      if (runeQuery.resultFormat === 'compactedList') {
        return {
          header: runeResult[0].columns,
          rows: [].concat(...runeResult.map(r => r.events))
        };
      }
      return processScan(runeResult);

    case 'segmentMetadata':
      return processSegmentMetadata(runeResult);

    default:
      throw new Error(`Should never get here.`);
  }
}
