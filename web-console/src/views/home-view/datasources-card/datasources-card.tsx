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

import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';

import { pluralIfNeeded, queryDruidSql, QueryManager } from '../../../utils';
import { HomeViewCard } from '../home-view-card/home-view-card';

export interface DatasourcesCardProps {
  noSqlMode: boolean;
}

export interface DatasourcesCardState {
  datasourceCountLoading: boolean;
  datasourceCount: number;
  datasourceCountError?: string;
}

export class DatasourcesCard extends React.PureComponent<
  DatasourcesCardProps,
  DatasourcesCardState
> {
  private datasourceQueryManager: QueryManager<boolean, any>;

  constructor(props: DatasourcesCardProps, context: any) {
    super(props, context);
    this.state = {
      datasourceCountLoading: false,
      datasourceCount: 0,
    };

    this.datasourceQueryManager = new QueryManager({
      processQuery: async noSqlMode => {
        let datasources: string[];
        if (!noSqlMode) {
          datasources = await queryDruidSql({
            query: `SELECT datasource FROM sys.segments GROUP BY 1`,
          });
        } else {
          const datasourcesResp = await axios.get('/druid/coordinator/v1/datasources');
          datasources = datasourcesResp.data;
        }
        return datasources.length;
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          datasourceCountLoading: loading,
          datasourceCount: result,
          datasourceCountError: error || undefined,
        });
      },
    });
  }

  componentDidMount(): void {
    const { noSqlMode } = this.props;

    this.datasourceQueryManager.runQuery(noSqlMode);
  }

  componentWillUnmount(): void {
    this.datasourceQueryManager.terminate();
  }

  render(): JSX.Element {
    const { datasourceCountLoading, datasourceCountError, datasourceCount } = this.state;
    return (
      <HomeViewCard
        className="datasources-card"
        href={'#datasources'}
        icon={IconNames.MULTI_SELECT}
        title={'Datasources'}
        loading={datasourceCountLoading}
        error={datasourceCountError}
      >
        {pluralIfNeeded(datasourceCount, 'datasource')}
      </HomeViewCard>
    );
  }
}
