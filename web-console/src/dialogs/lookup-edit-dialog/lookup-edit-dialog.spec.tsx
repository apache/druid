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

import { render } from '@testing-library/react';
import React from 'react';

import { isDisabled, LookupEditDialog } from './lookup-edit-dialog';

describe('lookup edit dialog', () => {
  it('matches snapshot', () => {
    const lookupEditDialog = (
      <LookupEditDialog
        onClose={() => {}}
        onSubmit={() => {}}
        onChange={() => {}}
        lookupName={'test'}
        lookupTier={'test'}
        lookupVersion={'test'}
        lookupSpec={{ type: 'map', map: {} }}
        isEdit={false}
        allLookupTiers={['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']}
      />
    );

    render(lookupEditDialog);
    expect(document.body.lastChild).toMatchSnapshot();
  });
});

describe('Type Map Should be disabled', () => {
  it('Missing LookupName', () => {
    expect(isDisabled(undefined, 'v1', '__default', { type: '' })).toBe(true);
  });

  it('Empty version', () => {
    expect(isDisabled('lookup', '', '__default', { type: '' })).toBe(true);
  });

  it('Missing version', () => {
    expect(isDisabled('lookup', undefined, '__default', { type: '' })).toBe(true);
  });

  it('Empty tier', () => {
    expect(isDisabled('lookup', 'v1', '', { type: '' })).toBe(true);
  });

  it('Missing tier', () => {
    expect(isDisabled('lookup', 'v1', undefined, { type: '' })).toBe(true);
  });

  it('Missing spec', () => {
    expect(isDisabled('lookup', 'v1', '__default', {})).toBe(true);
  });

  it('Type undefined', () => {
    expect(isDisabled('lookup', 'v1', '__default', { type: undefined })).toBe(true);
  });

  it('Lookup of type map with no map', () => {
    expect(isDisabled('lookup', 'v1', '__default', { type: 'map' })).toBe(true);
  });

  it('Lookup of type cachedNamespace with no extractionNamespace', () => {
    expect(isDisabled('lookup', 'v1', '__default', { type: 'cachedNamespace' })).toBe(true);
  });

  it('Lookup of type cachedNamespace with extractionNamespace type uri, format csv, no namespaceParseSpec', () => {
    expect(
      isDisabled('lookup', 'v1', '__default', {
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

  it('Lookup of type cachedNamespace with extractionNamespace type uri, format csv, no columns and skipHeaderRows', () => {
    expect(
      isDisabled('lookup', 'v1', '__default', {
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
      isDisabled('lookup', 'v1', '__default', {
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
      isDisabled('lookup', 'v1', '__default', {
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
      isDisabled('lookup', 'v1', '__default', {
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
    expect(isDisabled('lookup', 'v1', '__default', { type: 'cachedNamespace' })).toBe(true);
  });

  describe('ExtractionNamespace type URI', () => {
    it('Format csv, no namespaceParseSpec', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
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
        isDisabled('lookup', 'v1', '__default', {
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
        isDisabled('lookup', 'v1', '__default', {
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
        isDisabled('lookup', 'v1', '__default', {
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
        isDisabled('lookup', 'v1', '__default', {
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
    it('No namespace', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'jdbc',
            namespace: undefined,
            connectorConfig: {
              createTables: true,
              connectURI: 'jdbc:mysql://localhost:3306/druid',
              user: 'druid',
              password: 'diurd',
            },
            table: 'some_lookup_table',
            keyColumn: 'the_old_dim_value',
            valueColumn: 'the_new_dim_value',
            tsColumn: 'timestamp_column',
            pollPeriod: 600000,
          },
        }),
      ).toBe(true);
    });

    it('No connectorConfig', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'jdbc',
            namespace: 'some_lookup',
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
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'jdbc',
            namespace: 'some_lookup',
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
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'jdbc',
            namespace: 'some_lookup',
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

    it('No keyColumn', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'jdbc',
            namespace: 'some_lookup',
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
    expect(isDisabled('lookup', 'v1', '__default', { type: 'map', map: { a: 'b' } })).toBe(false);
  });
});

describe('Type cachedNamespace Should be enabled', () => {
  describe('ExtractionNamespace type URI', () => {
    it('Format csv with columns', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'csv',
              columns: ['key', 'value'],
            },
          },
        }),
      ).toBe(false);
    });

    it('Format csv with skipHeaderRows', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'csv',
              skipHeaderRows: 1,
            },
          },
        }),
      ).toBe(false);
    });

    it('Format tsv, only columns', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'uri',
            uriPrefix: 's3://bucket/some/key/prefix/',
            fileRegex: 'renames-[0-9]*\\.gz',
            namespaceParseSpec: {
              format: 'tsv',
              columns: ['key', 'value'],
            },
          },
        }),
      ).toBe(false);
    });

    it('Format tsv, keyFieldName and valueFieldName', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
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
          },
        }),
      ).toBe(false);
    });
  });

  describe('ExtractionNamespace type JDBC', () => {
    it('No namespace', () => {
      expect(
        isDisabled('lookup', 'v1', '__default', {
          type: 'cachedNamespace',
          extractionNamespace: {
            type: 'jdbc',
            namespace: 'lookup',
            connectorConfig: {
              createTables: true,
              connectURI: 'jdbc:mysql://localhost:3306/druid',
              user: 'druid',
              password: 'diurd',
            },
            table: 'some_lookup_table',
            keyColumn: 'the_old_dim_value',
            valueColumn: 'the_new_dim_value',
          },
        }),
      ).toBe(false);
    });
  });
});
