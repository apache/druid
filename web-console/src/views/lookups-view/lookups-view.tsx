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

import { Button, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import axios from 'axios';
import React from 'react';
import ReactTable from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  RefreshButton,
  TableColumnSelector,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog, LookupEditDialog } from '../../dialogs/';
import { AppToaster } from '../../singletons/toaster';
import { getDruidErrorMessage, LocalStorageKeys, QueryManager } from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';

import './lookups-view.scss';

const tableColumns: string[] = ['Lookup name', 'Tier', 'Type', 'Version', ACTION_COLUMN_LABEL];

const DEFAULT_LOOKUP_TIER: string = '__default';

export interface LookupsViewProps {}

export interface LookupsViewState {
  lookups?: any[];
  loadingLookups: boolean;
  lookupsError?: string;
  lookupsUninitialized: boolean;
  lookupEditDialogOpen: boolean;
  lookupEditName: string;
  lookupEditTier: string;
  lookupEditVersion: string;
  lookupEditSpec: string;
  isEdit: boolean;
  allLookupTiers: string[];

  deleteLookupName?: string;
  deleteLookupTier?: string;

  hiddenColumns: LocalStorageBackedArray<string>;
}

export class LookupsView extends React.PureComponent<LookupsViewProps, LookupsViewState> {
  private lookupsQueryManager: QueryManager<null, { lookupEntries: any[]; tiers: string[] }>;

  constructor(props: LookupsViewProps, context: any) {
    super(props, context);
    this.state = {
      lookups: [],
      loadingLookups: true,
      lookupsUninitialized: false,
      lookupEditDialogOpen: false,
      lookupEditTier: '',
      lookupEditName: '',
      lookupEditVersion: '',
      lookupEditSpec: '',
      isEdit: false,
      allLookupTiers: [],

      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.LOOKUP_TABLE_COLUMN_SELECTION,
      ),
    };

    this.lookupsQueryManager = new QueryManager({
      processQuery: async () => {
        const tiersResp = await axios.get('/druid/coordinator/v1/lookups/config?discover=true');
        const tiers =
          tiersResp.data && tiersResp.data.length > 0 ? tiersResp.data : [DEFAULT_LOOKUP_TIER];

        const lookupEntries: {}[] = [];
        const lookupResp = await axios.get('/druid/coordinator/v1/lookups/config/all');
        const lookupData = lookupResp.data;
        Object.keys(lookupData).map((tier: string) => {
          const lookupIds = lookupData[tier];
          Object.keys(lookupIds).map((id: string) => {
            lookupEntries.push({
              tier,
              id,
              version: lookupIds[id].version,
              spec: lookupIds[id].lookupExtractorFactory,
            });
          });
        });

        return {
          lookupEntries,
          tiers,
        };
      },
      onStateChange: ({ result, loading, error }) => {
        this.setState({
          lookups: result ? result.lookupEntries : undefined,
          loadingLookups: loading,
          lookupsError: error,
          lookupsUninitialized: error === 'Request failed with status code 404',
          allLookupTiers: result ? result.tiers : [],
        });
      },
    });
  }

  componentDidMount(): void {
    this.lookupsQueryManager.runQuery(null);
  }

  componentWillUnmount(): void {
    this.lookupsQueryManager.terminate();
  }

  private async initializeLookup() {
    try {
      await axios.post(`/druid/coordinator/v1/lookups/config`, {});
      this.lookupsQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: getDruidErrorMessage(e),
      });
    }
  }

  private async openLookupEditDialog(tier: string, id: string) {
    const { lookups } = this.state;
    if (!lookups) return;

    const target: any = lookups.find((lookupEntry: any) => {
      return lookupEntry.tier === tier && lookupEntry.id === id;
    });
    if (id === '') {
      this.setState(prevState => ({
        lookupEditName: '',
        lookupEditTier: prevState.allLookupTiers[0],
        lookupEditDialogOpen: true,
        lookupEditSpec: '',
        lookupEditVersion: new Date().toISOString(),
        isEdit: false,
      }));
    } else {
      this.setState({
        lookupEditName: id,
        lookupEditTier: tier,
        lookupEditDialogOpen: true,
        lookupEditSpec: JSON.stringify(target.spec, null, 2),
        lookupEditVersion: target.version,
        isEdit: true,
      });
    }
  }

  private handleChangeLookup = (field: string, value: string) => {
    this.setState({
      [field]: value,
    } as any);
  };

  private async submitLookupEdit() {
    const {
      lookupEditTier,
      lookupEditName,
      lookupEditSpec,
      lookupEditVersion,
      isEdit,
    } = this.state;
    let endpoint = '/druid/coordinator/v1/lookups/config';
    const specJson: any = JSON.parse(lookupEditSpec);
    let dataJson: any;
    if (isEdit) {
      endpoint = `${endpoint}/${lookupEditTier}/${lookupEditName}`;
      dataJson = {
        version: lookupEditVersion,
        lookupExtractorFactory: specJson,
      };
    } else {
      dataJson = {
        [lookupEditTier]: {
          [lookupEditName]: {
            version: lookupEditVersion,
            lookupExtractorFactory: specJson,
          },
        },
      };
    }
    try {
      await axios.post(endpoint, dataJson);
      this.setState({
        lookupEditDialogOpen: false,
      });
      this.lookupsQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: getDruidErrorMessage(e),
      });
    }
  }

  private getLookupActions(lookupTier: string, lookupId: string): BasicAction[] {
    return [
      {
        icon: IconNames.EDIT,
        title: 'Edit',
        onAction: () => this.openLookupEditDialog(lookupTier, lookupId),
      },
      {
        icon: IconNames.CROSS,
        title: 'Delete',
        intent: Intent.DANGER,
        onAction: () => this.setState({ deleteLookupTier: lookupTier, deleteLookupName: lookupId }),
      },
    ];
  }

  renderDeleteLookupAction() {
    const { deleteLookupTier, deleteLookupName } = this.state;
    if (!deleteLookupTier) return;

    return (
      <AsyncActionDialog
        action={async () => {
          await axios.delete(
            `/druid/coordinator/v1/lookups/config/${deleteLookupTier}/${deleteLookupName}`,
          );
        }}
        confirmButtonText="Delete lookup"
        successText="Lookup was deleted"
        failText="Could not delete lookup"
        intent={Intent.DANGER}
        onClose={() => {
          this.setState({ deleteLookupTier: undefined, deleteLookupName: undefined });
        }}
        onSuccess={() => {
          this.lookupsQueryManager.rerunLastQuery();
        }}
      >
        <p>{`Are you sure you want to delete the lookup '${deleteLookupName}'?`}</p>
      </AsyncActionDialog>
    );
  }

  renderLookupsTable() {
    const {
      lookups,
      loadingLookups,
      lookupsError,
      lookupsUninitialized,
      hiddenColumns,
    } = this.state;

    if (lookupsUninitialized) {
      return (
        <div className="init-div">
          <Button
            icon={IconNames.BUILD}
            text="Initialize lookups"
            onClick={() => this.initializeLookup()}
          />
        </div>
      );
    }

    return (
      <>
        <ReactTable
          data={lookups || []}
          loading={loadingLookups}
          noDataText={
            !loadingLookups && lookups && !lookups.length ? 'No lookups' : lookupsError || ''
          }
          filterable
          columns={[
            {
              Header: 'Lookup name',
              id: 'lookup_name',
              accessor: 'id',
              filterable: true,
              show: hiddenColumns.exists('Lookup name'),
            },
            {
              Header: 'Tier',
              id: 'tier',
              accessor: 'tier',
              filterable: true,
              show: hiddenColumns.exists('Tier'),
            },
            {
              Header: 'Type',
              id: 'type',
              accessor: 'spec.type',
              filterable: true,
              show: hiddenColumns.exists('Type'),
            },
            {
              Header: 'Version',
              id: 'version',
              accessor: 'version',
              filterable: true,
              show: hiddenColumns.exists('Version'),
            },
            {
              Header: ACTION_COLUMN_LABEL,
              id: ACTION_COLUMN_ID,
              width: ACTION_COLUMN_WIDTH,
              accessor: (row: any) => ({ id: row.id, tier: row.tier }),
              filterable: false,
              Cell: (row: any) => {
                const lookupId = row.value.id;
                const lookupTier = row.value.tier;
                const lookupActions = this.getLookupActions(lookupTier, lookupId);
                return <ActionCell actions={lookupActions} />;
              },
              show: hiddenColumns.exists(ACTION_COLUMN_LABEL),
            },
          ]}
          defaultPageSize={50}
        />
      </>
    );
  }

  renderLookupEditDialog() {
    const {
      lookupEditDialogOpen,
      allLookupTiers,
      lookupEditSpec,
      lookupEditTier,
      lookupEditName,
      lookupEditVersion,
      isEdit,
    } = this.state;
    if (!lookupEditDialogOpen) return;

    return (
      <LookupEditDialog
        onClose={() => this.setState({ lookupEditDialogOpen: false })}
        onSubmit={() => this.submitLookupEdit()}
        onChange={this.handleChangeLookup}
        lookupSpec={lookupEditSpec}
        lookupName={lookupEditName}
        lookupTier={lookupEditTier}
        lookupVersion={lookupEditVersion}
        isEdit={isEdit}
        allLookupTiers={allLookupTiers}
      />
    );
  }

  render(): JSX.Element {
    const { lookupsError, hiddenColumns } = this.state;

    return (
      <div className="lookups-view app-view">
        <ViewControlBar label="Lookups">
          <RefreshButton
            onRefresh={auto => this.lookupsQueryManager.rerunLastQuery(auto)}
            localStorageKey={LocalStorageKeys.LOOKUPS_REFRESH_RATE}
          />
          {!lookupsError && (
            <Button
              icon={IconNames.PLUS}
              text="Add lookup"
              onClick={() => this.openLookupEditDialog('', '')}
            />
          )}
          <TableColumnSelector
            columns={tableColumns}
            onChange={column =>
              this.setState(prevState => ({
                hiddenColumns: prevState.hiddenColumns.toggle(column),
              }))
            }
            tableColumnsHidden={hiddenColumns.storedArray}
          />
        </ViewControlBar>
        {this.renderLookupsTable()}
        {this.renderLookupEditDialog()}
        {this.renderDeleteLookupAction()}
      </div>
    );
  }
}
