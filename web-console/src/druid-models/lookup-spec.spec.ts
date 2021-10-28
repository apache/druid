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

import { isLookupInvalid } from './lookup-spec';

describe('lookup-spec', () => {
  describe('Type Map Should be disabled', () => {
    it('Missing LookupName', () => {
      expect(isLookupInvalid(undefined, 'v1', '__default', { type: '' })).toBe(true);
    });

    it('Empty version', () => {
      expect(isLookupInvalid('lookup', '', '__default', { type: '' })).toBe(true);
    });

    it('Missing version', () => {
      expect(isLookupInvalid('lookup', undefined, '__default', { type: '' })).toBe(true);
    });

    it('Empty tier', () => {
      expect(isLookupInvalid('lookup', 'v1', '', { type: '' })).toBe(true);
    });

    it('Missing tier', () => {
      expect(isLookupInvalid('lookup', 'v1', undefined, { type: '' })).toBe(true);
    });

    it('Missing spec', () => {
      expect(isLookupInvalid('lookup', 'v1', '__default', {})).toBe(true);
    });

    it('Type undefined', () => {
      expect(isLookupInvalid('lookup', 'v1', '__default', { type: undefined })).toBe(true);
    });

    it('Lookup of type map with no map', () => {
      expect(isLookupInvalid('lookup', 'v1', '__default', { type: 'map' })).toBe(true);
    });

    it('Lookup of type cachedNamespace with no extractionNamespace', () => {
      expect(isLookupInvalid('lookup', 'v1', '__default', { type: 'cachedNamespace' })).toBe(true);
    });

    it('Lookup of type cachedNamespace with extractionNamespace type uri, format csv, no namespaceParseSpec', () => {
      expect(
        isLookupInvalid('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            pollPeriod: 'PT5M',
          },
        }),
      ).toBe(true);
    });

    it('Lookup of type cachedNamespace with extractionNamespace type uri, format csv, no columns and no hasHeaderRow', () => {
      expect(
        isLookupInvalid('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'csv',
            },
            pollPeriod: 'PT5M',
          },
        }),
      ).toBe(true);
    });

    it('Lookup of type cachedNamespace with extractionNamespace type uri, format tsv, no columns', () => {
      expect(
        isLookupInvalid('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'tsv',
              skipHeaderRows: 0,
            },
            pollPeriod: 'PT5M',
          },
        }),
      ).toBe(true);
    });

    it('Lookup of type cachedNamespace with extractionNamespace type customJson, format tsv, no keyFieldName', () => {
      expect(
        isLookupInvalid('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'customJson',
              valueFieldName: 'value',
            },
            pollPeriod: 'PT5M',
          },
        }),
      ).toBe(true);
    });

    it('Lookup of type cachedNamespace with extractionNamespace type customJson, format customJson, no valueFieldName', () => {
      expect(
        isLookupInvalid('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'customJson',
              keyFieldName: 'key',
            },
            pollPeriod: 'PT5M',
          },
        }),
      ).toBe(true);
    });
  });

  describe('Type cachedNamespace should be disabled', () => {
    it('No extractionNamespace', () => {
      expect(isLookupInvalid('lookup', 'v1', '__default', { type: 'cachedNamespace' })).toBe(true);
    });

    describe('ExtractionNamespace type URI', () => {
      it('Format csv, no namespaceParseSpec', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              pollPeriod: 'PT5M',
            },
          }),
        ).toBe(true);
      });

      it('Format csv, no columns and skipHeaderRows', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'csv',
              },
              pollPeriod: 'PT5M',
            },
          }),
        ).toBe(true);
      });

      it('Format tsv, no columns', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'tsv',
                skipHeaderRows: 0,
              },
              pollPeriod: 'PT5M',
            },
          }),
        ).toBe(true);
      });

      it('Format tsv, no keyFieldName', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'customJson',
                valueFieldName: 'value',
              },
              pollPeriod: 'PT5M',
            },
          }),
        ).toBe(true);
      });

      it('Format customJson, no valueFieldName', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'customJson',
                keyFieldName: 'key',
              },
              pollPeriod: 'PT5M',
            },
          }),
        ).toBe(true);
      });
    });

    describe('ExtractionNamespace type JDBC', () => {
      it('No connectorConfig', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'jdbc',
              connectorConfig: undefined,
              table: 'some_lookup_table',
              keyColumn: 'the_old_dim_value',
              valueColumn: 'the_new_dim_value',
              tsColumn: 'timestamp_column',
              pollPeriod: 600000,
            },
          }),
        ).toBe(true);
      });

      it('No table', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'jdbc',
              connectorConfig: {
                createTables: true,
                connectURI: 'jdbc:mysql://localhost:3306/druid',
                user: 'druid',
                password: 'diurd',
              },
              table: undefined,
              keyColumn: 'the_old_dim_value',
              valueColumn: 'the_new_dim_value',
              tsColumn: 'timestamp_column',
              pollPeriod: 600000,
            },
          }),
        ).toBe(true);
      });

      it('No keyColumn', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'jdbc',
              connectorConfig: {
                createTables: true,
                connectURI: 'jdbc:mysql://localhost:3306/druid',
                user: 'druid',
                password: 'diurd',
              },
              table: 'some_lookup_table',
              keyColumn: undefined,
              valueColumn: 'the_new_dim_value',
              tsColumn: 'timestamp_column',
              pollPeriod: 600000,
            },
          }),
        ).toBe(true);
      });

      it('No valueColumn', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'jdbc',
              connectorConfig: {
                createTables: true,
                connectURI: 'jdbc:mysql://localhost:3306/druid',
                user: 'druid',
                password: 'diurd',
              },
              table: 'some_lookup_table',
              keyColumn: 'the_old_dim_value',
              valueColumn: undefined,
              tsColumn: 'timestamp_column',
              pollPeriod: 600000,
            },
          }),
        ).toBe(true);
      });
    });
  });

  describe('Type Map Should be enabled', () => {
    it('Has type and has Map', () => {
      expect(isLookupInvalid('lookup', 'v1', '__default', { type: 'map', map: { a: 'b' } })).toBe(
        false,
      );
    });
  });

  describe('Type cachedNamespace Should be enabled', () => {
    describe('ExtractionNamespace type URI', () => {
      it('Format csv with columns', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'csv',
                columns: ['key', 'value'],
              },
              pollPeriod: 'PT1H',
            },
          }),
        ).toBe(false);
      });

      it('Format csv with hasHeaderRow', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'csv',
                hasHeaderRow: true,
              },
              pollPeriod: 'PT1H',
            },
          }),
        ).toBe(false);
      });

      it('Format tsv, only columns', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'tsv',
                columns: ['key', 'value'],
              },
              pollPeriod: 'PT1H',
            },
          }),
        ).toBe(false);
      });

      it('Format tsv, keyFieldName and valueFieldName', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'uri',
              uriPrefix: 's3://bucket/some/key/prefix/',
              fileRegex: 'renames-[0-9]*\\.gz',
              namespaceParseSpec: {
                format: 'customJson',
                valueFieldName: 'value',
                keyFieldName: 'value',
              },
              pollPeriod: 'PT1H',
            },
          }),
        ).toBe(false);
      });
    });

    describe('ExtractionNamespace type JDBC', () => {
      it('All good', () => {
        expect(
          isLookupInvalid('lookup', 'v1', '__default', {
            type: 'cachedNamespace',
            extractionNamespace: {
              type: 'jdbc',
              connectorConfig: {
                createTables: true,
                connectURI: 'jdbc:mysql://localhost:3306/druid',
                user: 'druid',
                password: 'diurd',
              },
              table: 'some_lookup_table',
              keyColumn: 'the_old_dim_value',
              valueColumn: 'the_new_dim_value',
              pollPeriod: 'PT1H',
            },
          }),
        ).toBe(false);
      });
    });
  });
});
