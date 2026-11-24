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

import * as path from 'path';

import { loadData } from './component/load-data/data-loader';
import { expect, test } from './util/fixtures';
import { CLUSTER_STATE_POLL, getDatasourceSegments, getTaskStatuses } from './util/sql';

// The data/ dir of embedded-tests' test resources, which has the same 10 rows of Wikipedia edits (over 2 days) in each
// format, in 3 files. InputFormatsWebConsoleTest (in embedded-tests) sets it, on a cluster with the extensions that
// read Avro, Parquet and ORC.
const DATA_DIR = process.env['DRUID_E2E_TEST_DATA_DIR'];

const FORMATS = [
  { inputFormat: 'csv', dir: 'csv', file: 'wikipedia_index_data*.csv', text: true },
  { inputFormat: 'tsv', dir: 'tsv', file: 'wikipedia_index_data*.tsv', text: true },
  { inputFormat: 'parquet', dir: 'parquet', file: 'wikipedia_index_data*.parquet', text: false },
  { inputFormat: 'orc', dir: 'orc', file: 'wikipedia_index_data*.orc', text: false },
  { inputFormat: 'avro_ocf', dir: 'avro', file: 'wikipedia_index_data*.avro', text: false },
];

test.describe('Input formats', () => {
  test.skip(
    !DATA_DIR,
    'needs DRUID_E2E_TEST_DATA_DIR (run by InputFormatsWebConsoleTest in embedded-tests)',
  );

  for (const format of FORMATS) {
    test(`Loads ${format.inputFormat} data`, async ({ page, request, newDatasourceName }) => {
      const datasourceName = newDatasourceName(`input-format-${format.inputFormat}`);
      await loadData(page, {
        connector: {
          type: 'local',
          baseDirectory: path.join(DATA_DIR!, format.dir),
          fileFilter: format.file,
        },
        validateConnect: lines => {
          // The text formats show their lines (a header and the rows of each file), the others their bytes
          if (format.text) {
            expect(lines).toHaveLength(13);
            expect(lines[0]).toMatch(/^timestamp.page.language.user/);
          } else {
            expect(lines.length).toBeGreaterThan(0);
          }
        },
        validateParseData: ({ inputFormat, columns }) => {
          // Picked by the data loader, from the sampled data
          expect(inputFormat).toBe(format.inputFormat);
          expect(columns).toEqual(
            expect.arrayContaining(['timestamp', 'page', 'user', 'added', 'deleted', 'delta']),
          );
        },
        rollup: false,
        segmentGranularity: 'day',
        datasourceName,
      });

      await expect
        .poll(() => getTaskStatuses(request, datasourceName), CLUSTER_STATE_POLL)
        .toEqual(['SUCCESS']);
      await expect
        .poll(() => getDatasourceSegments(request, datasourceName), CLUSTER_STATE_POLL)
        .toEqual({ numSegments: 2, numAvailableSegments: 2, numRows: 10 });
    });
  }
});
