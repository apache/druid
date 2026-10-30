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

import { Api } from '../../singletons';

import { SupervisorToSqlDialog } from './supervisor-to-sql-dialog';

jest.mock('../../singletons', () => ({
  Api: {
    instance: {
      get: jest.fn(),
      encodePath: jest.fn((path: string) => encodeURIComponent(path)),
    },
  },
}));

describe('SupervisorToSqlDialog', () => {
  const mockOnConvert = jest.fn();
  const mockOnClose = jest.fn();

  const mockSupervisorList = ['wikipedia-kafka', 'clickstream-kafka'];

  const mockSupervisorSpec = {
    type: 'kafka',
    spec: {
      dataSchema: {
        dataSource: 'wikipedia',
        timestampSpec: {
          column: 'timestamp',
          format: 'iso',
        },
        dimensionsSpec: {
          dimensions: [
            'channel',
            'user',
            { name: 'page', type: 'string' },
            { name: 'namespace', type: 'string' },
          ],
        },
        metricsSpec: [
          { name: 'count', type: 'count' },
          { name: 'added', type: 'longSum', fieldName: 'added' },
        ],
      },
      ioConfig: {
        topic: 'wikipedia',
        inputSource: {
          type: 's3',
          uris: ['s3://my-bucket/wikipedia/data/'],
        },
      },
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();
    (Api.instance.get as jest.Mock).mockImplementation((url: string) => {
      if (url === '/druid/indexer/v1/supervisor') {
        return Promise.resolve({ data: mockSupervisorList });
      }
      if (url.includes('/druid/indexer/v1/supervisor/')) {
        return Promise.resolve({ data: mockSupervisorSpec });
      }
      return Promise.reject(new Error('Unknown endpoint'));
    });
  });

  it('renders the dialog', () => {
    const { container } = render(
      React.createElement(SupervisorToSqlDialog, {
        onConvert: mockOnConvert,
        onClose: mockOnClose,
      }),
    );
    expect(container).toBeTruthy();
  });

  it('calls onClose when Close button is clicked', () => {
    const { getByText } = render(
      React.createElement(SupervisorToSqlDialog, {
        onConvert: mockOnConvert,
        onClose: mockOnClose,
      }),
    );

    const closeButton = getByText('Close');
    closeButton.click();

    expect(mockOnClose).toHaveBeenCalled();
  });
});
