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
import { LookupTableActionDialog } from '../../dialogs/lookup-table-action-dialog/lookup-table-action-dialog';
import { LookupSpec } from '../../druid-models';
import { AppToaster } from '../../singletons/toaster';
import {
  getDruidErrorMessage,
  isLookupsUninitialized,
  LocalStorageKeys,
  QueryManager,
  QueryState,
} from '../../utils';
import { BasicAction } from '../../utils/basic-action';
import { LocalStorageBackedArray } from '../../utils/local-storage-backed-array';

import './lookups-view.scss';

const tableColumns: string[] = [
  'Lookup name',
  'Lookup tier',
  'Type',
  'Version',
  ACTION_COLUMN_LABEL,
];

const DEFAULT_LOOKUP_TIER: string = '__default';

function tierNameCompare(a: string, b: string) {
  return a.localeCompare(b);
}

export interface LookupEntriesAndTiers {
  lookupEntries: any[];
  tiers: string[];
}

export interface LookupEditInfo {
  name: string;
  tier: string;
  version: string;
  spec: LookupSpec;
}

export interface LookupsViewProps {}

export interface LookupsViewState {
  lookupEntriesAndTiersState: QueryState<LookupEntriesAndTiers>;

  lookupEdit?: LookupEditInfo;
  isEdit: boolean;

  deleteLookupName?: string;
  deleteLookupTier?: string;

  hiddenColumns: LocalStorageBackedArray<string>;

  lookupTableActionDialogId?: string;
  actions: BasicAction[];
}

export class LookupsView extends React.PureComponent<LookupsViewProps, LookupsViewState> {
  private lookupsQueryManager: QueryManager<null, LookupEntriesAndTiers>;

  constructor(props: LookupsViewProps, context: any) {
    super(props, context);
    this.state = {
      lookupEntriesAndTiersState: QueryState.INIT,
      isEdit: false,
      actions: [],

      hiddenColumns: new LocalStorageBackedArray<string>(
        LocalStorageKeys.LOOKUP_TABLE_COLUMN_SELECTION,
      ),
    };

    this.lookupsQueryManager = new QueryManager({
      processQuery: async () => {
        const tiersResp = await axios.get('/druid/coordinator/v1/lookups/config?discover=true');
        const tiers =
          tiersResp.data && tiersResp.data.length > 0
            ? tiersResp.data.sort(tierNameCompare)
            : [DEFAULT_LOOKUP_TIER];

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
      onStateChange: lookupEntriesAndTiersState => {
        this.setState({
          lookupEntriesAndTiersState,
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
    const { lookupEntriesAndTiersState } = this.state;
    const lookupEntriesAndTiers = lookupEntriesAndTiersState.data;
    if (!lookupEntriesAndTiers) return;

    const target: any = lookupEntriesAndTiers.lookupEntries.find(lookupEntry => {
      return lookupEntry.tier === tier && lookupEntry.id === id;
    });
    if (id === '') {
      this.setState(prevState => {
        const { lookupEntriesAndTiersState } = prevState;
        const loadingEntriesAndTiers = lookupEntriesAndTiersState.data;
        return {
          isEdit: false,
          lookupEdit: {
            name: '',
            tier: loadingEntriesAndTiers ? loadingEntriesAndTiers.tiers[0] : '',
            spec: { type: 'map', map: {} },
            version: new Date().toISOString(),
          },
        };
      });
    } else {
      this.setState({
        isEdit: true,
        lookupEdit: {
          name: id,
          tier: tier,
          spec: target.spec,
          version: target.version,
        },
      });
    }
  }

  private handleChangeLookup = (field: keyof LookupEditInfo, value: string | LookupSpec) => {
    this.setState(state => ({
      lookupEdit: Object.assign({}, state.lookupEdit, { [field]: value }),
    }));
  };

  private async submitLookupEdit(updateLookupVersion: boolean) {
    const { lookupEdit, isEdit } = this.state;
    if (!lookupEdit) return;

    const version = updateLookupVersion ? new Date().toISOString() : lookupEdit.version;
    let endpoint = '/druid/coordinator/v1/lookups/config';
    const specJson: any = lookupEdit.spec;
    let dataJson: any;
    if (isEdit) {
      endpoint = `${endpoint}/${lookupEdit.tier}/${lookupEdit.name}`;
      dataJson = {
        version: version,
        lookupExtractorFactory: specJson,
      };
    } else {
      dataJson = {
        [lookupEdit.tier]: {
          [lookupEdit.name]: {
            version: version,
            lookupExtractorFactory: specJson,
          },
        },
      };
    }
    try {
      await axios.post(endpoint, dataJson);
      this.setState({
        lookupEdit: undefined,
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
    const { lookupEntriesAndTiersState, hiddenColumns } = this.state;
    const lookupEntriesAndTiers = lookupEntriesAndTiersState.data;
    const lookups = lookupEntriesAndTiers ? lookupEntriesAndTiers.lookupEntries : undefined;

    if (isLookupsUninitialized(lookupEntriesAndTiersState.error)) {
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
          loading={lookupEntriesAndTiersState.loading}
          noDataText={
            !lookupEntriesAndTiersState.loading && lookups && !lookups.length
              ? 'No lookups'
              : lookupEntriesAndTiersState.getErrorMessage() || ''
          }
          filterable
          columns={[
            {
              Header: 'Lookup name',
              show: hiddenColumns.exists('Lookup name'),
              id: 'lookup_name',
              accessor: 'id',
              filterable: true,
            },
            {
              Header: 'Lookup tier',
              show: hiddenColumns.exists('Lookup tier'),
              id: 'tier',
              accessor: 'tier',
              filterable: true,
            },
            {
              Header: 'Type',
              show: hiddenColumns.exists('Type'),
              id: 'type',
              accessor: 'spec.type',
              filterable: true,
            },
            {
              Header: 'Version',
              show: hiddenColumns.exists('Version'),
              id: 'version',
              accessor: 'version',
              filterable: true,
            },
            {
              Header: ACTION_COLUMN_LABEL,
              show: hiddenColumns.exists(ACTION_COLUMN_LABEL),
              id: ACTION_COLUMN_ID,
              width: ACTION_COLUMN_WIDTH,
              accessor: (row: any) => ({ id: row.id, tier: row.tier }),
              filterable: false,
              Cell: (row: any) => {
                const lookupId = row.value.id;
                const lookupTier = row.value.tier;
                const lookupActions = this.getLookupActions(lookupTier, lookupId);
                return (
                  <ActionCell
                    onDetail={() => {
                      this.setState({
                        lookupTableActionDialogId: lookupId,
                        actions: lookupActions,
                      });
                    }}
                    actions={lookupActions}
                  />
                );
              },
            },
          ]}
          defaultPageSize={50}
        />
      </>
    );
  }

  renderLookupEditDialog() {
    const { lookupEdit, isEdit, lookupEntriesAndTiersState } = this.state;
    if (!lookupEdit) return;
    const allLookupTiers = lookupEntriesAndTiersState.data
      ? lookupEntriesAndTiersState.data.tiers
      : [];

    return (
      <LookupEditDialog
        onClose={() => this.setState({ lookupEdit: undefined })}
        onSubmit={updateLookupVersion => this.submitLookupEdit(updateLookupVersion)}
        onChange={this.handleChangeLookup}
        lookupSpec={lookupEdit.spec}
        lookupName={lookupEdit.name}
        lookupTier={lookupEdit.tier}
        lookupVersion={lookupEdit.version}
        isEdit={isEdit}
        allLookupTiers={allLookupTiers}
      />
    );
  }

  render(): JSX.Element {
    const {
      lookupEntriesAndTiersState,
      hiddenColumns,
      lookupTableActionDialogId,
      actions,
    } = this.state;

    return (
      <div className="lookups-view app-view">
        <ViewControlBar label="Lookups">
          <RefreshButton
            onRefresh={auto => this.lookupsQueryManager.rerunLastQuery(auto)}
            localStorageKey={LocalStorageKeys.LOOKUPS_REFRESH_RATE}
          />
          {!lookupEntriesAndTiersState.isError() && (
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
        {lookupTableActionDialogId && (
          <LookupTableActionDialog
            lookupId={lookupTableActionDialogId}
            actions={actions}
            onClose={() => this.setState({ lookupTableActionDialogId: undefined })}
          />
        )}
      </div>
    );
  }
}
