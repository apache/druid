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

import { Button, Icon, Intent, Tag } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';
import type { Filter } from 'react-table';
import ReactTable from 'react-table';

import {
  ACTION_COLUMN_ID,
  ACTION_COLUMN_LABEL,
  ACTION_COLUMN_WIDTH,
  ActionCell,
  RefreshButton,
  TableClickableCell,
  TableColumnSelector,
  TableFilterableCell,
  ViewControlBar,
} from '../../components';
import { AsyncActionDialog, LookupEditDialog } from '../../dialogs/';
import { LookupTableActionDialog } from '../../dialogs/lookup-table-action-dialog/lookup-table-action-dialog';
import type { LookupSpec } from '../../druid-models';
import { lookupSpecSummary } from '../../druid-models';
import { STANDARD_TABLE_PAGE_SIZE, STANDARD_TABLE_PAGE_SIZE_OPTIONS } from '../../react-table';
import { Api, AppToaster } from '../../singletons';
import {
  deepGet,
  getApiArray,
  getDruidErrorMessage,
  hasOverlayOpen,
  isLookupsUninitialized,
  LocalStorageBackedVisibility,
  LocalStorageKeys,
  QueryManager,
  QueryState,
} from '../../utils';
import type { BasicAction } from '../../utils/basic-action';

import './lookups-view.scss';

const tableColumns: string[] = [
  'Lookup name',
  'Lookup tier',
  'Type',
  'Version',
  'Poll period',
  'Summary',
];

const DEFAULT_LOOKUP_TIER = '__default';

function tierNameCompare(a: string, b: string) {
  return a.localeCompare(b);
}

export interface LookupEntriesAndTiers {
  lookupEntries: LookupEntry[];
  tiers: string[];
}

export interface LookupEntry {
  id: string;
  tier: string;
  version: string;
  spec: LookupSpec;
}

export interface LookupEditInfo {
  id: string;
  tier: string;
  version: string;
  spec: Partial<LookupSpec>;
}

export interface LookupsViewProps {
  filters: Filter[];
  onFiltersChange(filters: Filter[]): void;
}

export interface LookupsViewState {
  lookupEntriesAndTiersState: QueryState<LookupEntriesAndTiers>;

  lookupEdit?: LookupEditInfo;
  isEdit: boolean;

  deleteLookupName?: string;
  deleteLookupTier?: string;

  visibleColumns: LocalStorageBackedVisibility;

  lookupTableActionDialogId?: string;
  actions: BasicAction[];
}

export class LookupsView extends React.PureComponent<LookupsViewProps, LookupsViewState> {
  private readonly lookupsQueryManager: QueryManager<null, LookupEntriesAndTiers>;

  constructor(props: LookupsViewProps) {
    super(props);
    this.state = {
      lookupEntriesAndTiersState: QueryState.INIT,
      isEdit: false,
      actions: [],

      visibleColumns: new LocalStorageBackedVisibility(
        LocalStorageKeys.LOOKUP_TABLE_COLUMN_SELECTION,
      ),
    };

    this.lookupsQueryManager = new QueryManager({
      processQuery: async (_, cancelToken) => {
        const tiersResp = await getApiArray(
          '/druid/coordinator/v1/lookups/config?discover=true',
          cancelToken,
        );
        const tiers =
          tiersResp.length > 0 ? tiersResp.sort(tierNameCompare) : [DEFAULT_LOOKUP_TIER];

        const lookupResp = await Api.instance.get('/druid/coordinator/v1/lookups/config/all', {
          cancelToken,
        });
        const lookupData = lookupResp.data;

        const lookupEntries: LookupEntry[] = [];
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
      await Api.instance.post(`/druid/coordinator/v1/lookups/config`, {});
      this.lookupsQueryManager.rerunLastQuery();
    } catch (e) {
      AppToaster.show({
        icon: IconNames.ERROR,
        intent: Intent.DANGER,
        message: getDruidErrorMessage(e),
      });
    }
  }

  private openLookupEditDialog(tier: string, id: string) {
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
            id: '',
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
          id,
          tier,
          spec: target.spec,
          version: target.version,
        },
      });
    }
  }

  private readonly handleChangeLookup = <K extends keyof LookupEditInfo>(
    field: K,
    value: LookupEditInfo[K],
  ) => {
    this.setState(state => ({
      lookupEdit: { ...state.lookupEdit!, [field]: value },
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
      endpoint = `${endpoint}/${lookupEdit.tier}/${lookupEdit.id}`;
      dataJson = {
        version: version,
        lookupExtractorFactory: specJson,
      };
    } else {
      dataJson = {
        [lookupEdit.tier]: {
          [lookupEdit.id]: {
            version: version,
            lookupExtractorFactory: specJson,
          },
        },
      };
    }
    try {
      await Api.instance.post(endpoint, dataJson);
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

  private renderDeleteLookupAction() {
    const { deleteLookupTier, deleteLookupName } = this.state;
    if (!deleteLookupTier || !deleteLookupName) return;

    return (
      <AsyncActionDialog
        action={async () => {
          await Api.instance.delete(
            `/druid/coordinator/v1/lookups/config/${Api.encodePath(
              deleteLookupTier,
            )}/${Api.encodePath(deleteLookupName)}`,
          );
        }}
        confirmButtonText="Delete lookup"
        successText={
          <>
            Lookup <Tag minimal>{deleteLookupName}</Tag> was deleted
          </>
        }
        failText={
          <>
            Could not delete lookup <Tag minimal>{deleteLookupName}</Tag>
          </>
        }
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

  private onDetail(lookup: LookupEntry): void {
    const lookupId = lookup.id;
    const lookupTier = lookup.tier;
    this.setState({
      lookupTableActionDialogId: lookupId,
      actions: this.getLookupActions(lookupTier, lookupId),
    });
  }

  private renderFilterableCell(field: string) {
    const { filters, onFiltersChange } = this.props;

    return function FilterableCell(row: { value: any }) {
      return (
        <TableFilterableCell
          field={field}
          value={row.value}
          filters={filters}
          onFiltersChange={onFiltersChange}
        />
      );
    };
  }

  private renderLookupsTable() {
    const { filters, onFiltersChange } = this.props;
    const { lookupEntriesAndTiersState, visibleColumns } = this.state;
    const lookupEntriesAndTiers = lookupEntriesAndTiersState.data;
    const lookups = lookupEntriesAndTiers ? lookupEntriesAndTiers.lookupEntries : [];

    if (isLookupsUninitialized(lookupEntriesAndTiersState.error)) {
      return (
        <div className="init-pane">
          <Button
            icon={IconNames.BUILD}
            text="Initialize lookups"
            onClick={() => void this.initializeLookup()}
            large
            intent={Intent.PRIMARY}
          />
        </div>
      );
    }

    return (
      <ReactTable
        data={lookups}
        loading={lookupEntriesAndTiersState.loading}
        noDataText={lookupEntriesAndTiersState.getErrorMessage() || 'No lookups'}
        filterable
        filtered={filters}
        onFilteredChange={onFiltersChange}
        defaultSorted={[{ id: 'lookup_name', desc: false }]}
        defaultPageSize={STANDARD_TABLE_PAGE_SIZE}
        pageSizeOptions={STANDARD_TABLE_PAGE_SIZE_OPTIONS}
        showPagination={lookups.length > STANDARD_TABLE_PAGE_SIZE}
        columns={[
          {
            Header: 'Lookup name',
            show: visibleColumns.shown('Lookup name'),
            id: 'lookup_name',
            accessor: 'id',
            filterable: true,
            width: 200,
            Cell: ({ value, original }) => (
              <TableClickableCell
                tooltip="Show detail"
                onClick={() => this.onDetail(original)}
                hoverIcon={IconNames.SEARCH_TEMPLATE}
              >
                {value}
              </TableClickableCell>
            ),
          },
          {
            Header: 'Lookup tier',
            show: visibleColumns.shown('Lookup tier'),
            id: 'tier',
            accessor: 'tier',
            filterable: true,
            width: 100,
            Cell: this.renderFilterableCell('tier'),
          },
          {
            Header: 'Type',
            show: visibleColumns.shown('Type'),
            id: 'type',
            accessor: 'spec.type',
            filterable: true,
            width: 150,
            Cell: this.renderFilterableCell('type'),
          },
          {
            Header: 'Version',
            show: visibleColumns.shown('Version'),
            id: 'version',
            accessor: 'version',
            filterable: true,
            width: 190,
            Cell: this.renderFilterableCell('version'),
          },
          {
            Header: 'Poll period',
            show: visibleColumns.shown('Poll period'),
            id: 'poolPeriod',
            width: 150,
            className: 'padded',
            accessor: row => deepGet(row, 'spec.extractionNamespace.pollPeriod'),
            Cell: ({ original }) => {
              const { type } = original.spec;
              if (type === 'map') return 'Static map';
              if (type === 'kafka') return 'Kafka based';
              const pollPeriod = deepGet(original, 'spec.extractionNamespace.pollPeriod');
              if (!pollPeriod) {
                return (
                  <>
                    <Icon icon={IconNames.WARNING_SIGN} intent={Intent.WARNING} /> No poll period
                    set
                  </>
                );
              }
              return pollPeriod;
            },
          },
          {
            Header: 'Summary',
            show: visibleColumns.shown('Summary'),
            id: 'summary',
            accessor: row => lookupSpecSummary(row.spec),
            width: 600,
            Cell: this.renderFilterableCell('summary'),
          },
          {
            Header: ACTION_COLUMN_LABEL,
            id: ACTION_COLUMN_ID,
            width: ACTION_COLUMN_WIDTH,
            filterable: false,
            sortable: false,
            accessor: 'id',
            Cell: ({ original }) => {
              const lookupId = original.id;
              const lookupTier = original.tier;
              const lookupActions = this.getLookupActions(lookupTier, lookupId);
              return (
                <ActionCell
                  onDetail={() => {
                    this.onDetail(original);
                  }}
                  actions={lookupActions}
                  menuTitle={lookupId}
                />
              );
            },
          },
        ]}
      />
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
        onSubmit={updateLookupVersion => void this.submitLookupEdit(updateLookupVersion)}
        onChange={this.handleChangeLookup}
        lookupId={lookupEdit.id}
        lookupTier={lookupEdit.tier}
        lookupVersion={lookupEdit.version}
        lookupSpec={lookupEdit.spec}
        isEdit={isEdit}
        allLookupTiers={allLookupTiers}
      />
    );
  }

  render() {
    const { lookupEntriesAndTiersState, visibleColumns, lookupTableActionDialogId, actions } =
      this.state;

    return (
      <div className="lookups-view app-view">
        <ViewControlBar label="Lookups">
          <RefreshButton
            onRefresh={auto => {
              if (auto && hasOverlayOpen()) return;
              this.lookupsQueryManager.rerunLastQuery(auto);
            }}
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
                visibleColumns: prevState.visibleColumns.toggle(column),
              }))
            }
            tableColumnsHidden={visibleColumns.getHiddenColumns()}
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
