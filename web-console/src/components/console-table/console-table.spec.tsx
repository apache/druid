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

import { fireEvent, render } from '@testing-library/react';

import type { ConsoleTableColumn } from './console-table';
import { ConsoleTable } from './console-table';

interface Pet {
  name: string;
  kind: string;
  age: number | null;
  owner: { name: string };
}

const PETS: Pet[] = [
  { name: 'Rex', kind: 'dog', age: 3, owner: { name: 'Ann' } },
  { name: 'Tom', kind: 'cat', age: 5, owner: { name: 'Bob' } },
  { name: 'Fido', kind: 'dog', age: null, owner: { name: 'Cid' } },
  { name: 'Kitty', kind: 'cat', age: 5, owner: { name: 'Ann' } },
  { name: 'Nemo', kind: 'fish', age: 1, owner: { name: 'Bob' } },
];

const COLUMNS: ConsoleTableColumn<Pet>[] = [
  { Header: 'Name', accessor: 'name', width: 100 },
  { Header: 'Kind', accessor: 'kind', width: 80, className: 'padded' },
  { Header: 'Age', accessor: 'age' },
  { Header: 'Owner', id: 'owner', accessor: 'owner.name', show: false },
];

function getRows(container: HTMLElement): string[] {
  return Array.from(container.querySelectorAll('.ct-tbody .ct-tr:not(.-padRow)')).map(tr =>
    Array.from(tr.querySelectorAll('.ct-td'))
      .map(td => td.textContent)
      .join('|'),
  );
}

function getHeader(container: HTMLElement, text: string): HTMLElement {
  const header = Array.from(container.querySelectorAll('.ct-thead.-header .ct-th')).find(
    th => th.textContent === text,
  );
  if (!header) throw new Error(`no header ${text}`);
  return header as HTMLElement;
}

function typeFilter(container: HTMLElement, columnIndex: number, value: string) {
  const input = container
    .querySelectorAll('.ct-thead.-filters .ct-th')
    [columnIndex].querySelector('input')!;
  fireEvent.change(input, { target: { value } });
  fireEvent.keyDown(input, { key: 'Enter', target: { value } });
  fireEvent.blur(input);
}

describe('ConsoleTable', () => {
  it('matches snapshot', () => {
    const { container } = render(
      <ConsoleTable
        data={PETS.slice(0, 2)}
        filterable
        defaultPageSize={3}
        columns={[
          { Header: 'Name', accessor: 'name', width: 100 },
          {
            Header: 'Details',
            headerClassName: 'details',
            columns: [
              { Header: 'Kind', accessor: 'kind', minWidth: 60, maxWidth: 200 },
              { Header: 'Age', accessor: 'age', filterable: false, resizable: false },
            ],
          },
        ]}
      />,
    );
    expect(container.firstChild).toMatchSnapshot();
  });

  it('renders cells, hides columns, and pads the page', () => {
    const { container } = render(
      <ConsoleTable
        data={PETS}
        defaultPageSize={7}
        columns={[
          ...COLUMNS,
          {
            Header: 'Summary',
            id: 'summary',
            accessor: d => d.name.length,
            Cell: ({ value, original, row }) => `${original.name}: ${value} (${row.kind})`,
          },
        ]}
      />,
    );
    expect(getRows(container)[0]).toEqual('Rex|dog|3|Rex: 3 (dog)');
    expect(container.querySelectorAll('.ct-thead.-header .ct-th')).toHaveLength(4);
    expect(container.querySelectorAll('.ct-tr.-padRow')).toHaveLength(2);
    expect(container.querySelector('.ct-tbody .ct-td')!.getAttribute('style')).toEqual(
      'flex: 100 0 auto; width: 100px; max-width: 100px;',
    );
  });

  it('sorts', () => {
    const onSortedChange = jest.fn();
    const { container } = render(
      <ConsoleTable data={PETS} columns={COLUMNS} onSortedChange={onSortedChange} />,
    );

    fireEvent.click(getHeader(container, 'Age'));
    expect(onSortedChange).toHaveBeenLastCalledWith([{ id: 'age', desc: false }]);
    // null values go first in ascending order, ties keep the data order
    expect(getRows(container).map(r => r.split('|')[0])).toEqual([
      'Fido',
      'Nemo',
      'Rex',
      'Tom',
      'Kitty',
    ]);
    expect(getHeader(container, 'Age').className).toContain('-sort-asc');

    fireEvent.click(getHeader(container, 'Age'));
    expect(onSortedChange).toHaveBeenLastCalledWith([{ id: 'age', desc: true }]);
    // ties are in the reverse data order when sorting in descending order
    expect(getRows(container).map(r => r.split('|')[0])).toEqual([
      'Kitty',
      'Tom',
      'Rex',
      'Nemo',
      'Fido',
    ]);

    fireEvent.click(getHeader(container, 'Name'), { shiftKey: true });
    expect(onSortedChange).toHaveBeenLastCalledWith([
      { id: 'age', desc: true },
      { id: 'name', desc: false },
    ]);
    expect(getRows(container).map(r => r.split('|')[0])).toEqual([
      'Kitty',
      'Tom',
      'Rex',
      'Nemo',
      'Fido',
    ]);

    fireEvent.click(getHeader(container, 'Name'), { shiftKey: true });
    fireEvent.click(getHeader(container, 'Name'), { shiftKey: true });
    expect(onSortedChange).toHaveBeenLastCalledWith([{ id: 'age', desc: true }]);
  });

  it('uses custom sort methods and defaultSortDesc', () => {
    const kindOrder = ['fish', 'dog', 'cat'];
    const { container } = render(
      <ConsoleTable
        data={PETS}
        defaultSorted={[{ id: 'kind', desc: false }]}
        columns={[
          { Header: 'Name', accessor: 'name', defaultSortDesc: true },
          {
            Header: 'Kind',
            accessor: 'kind',
            sortMethod: (a, b) => kindOrder.indexOf(a) - kindOrder.indexOf(b),
          },
          { Header: 'Age', accessor: 'age', sortable: false },
        ]}
      />,
    );
    expect(getRows(container).map(r => r.split('|')[1])).toEqual([
      'fish',
      'dog',
      'dog',
      'cat',
      'cat',
    ]);

    fireEvent.click(getHeader(container, 'Age'));
    expect(getHeader(container, 'Age').className).not.toContain('-sort');

    fireEvent.click(getHeader(container, 'Name'));
    expect(getRows(container).map(r => r.split('|')[0])).toEqual([
      'Tom',
      'Rex',
      'Nemo',
      'Kitty',
      'Fido',
    ]);
  });

  it('filters', () => {
    const onFilteredChange = jest.fn();
    const { container } = render(
      <ConsoleTable
        data={PETS}
        filterable
        onFilteredChange={onFilteredChange}
        columns={[
          ...COLUMNS,
          {
            Header: 'Name length',
            id: 'nameLength',
            accessor: d => d.name.length,
            filterMethod: (filter, row) => `~${row.nameLength}` === filter.value,
          },
        ]}
      />,
    );

    typeFilter(container, 1, 'd');
    expect(onFilteredChange).toHaveBeenLastCalledWith([{ id: 'kind', value: '~d' }]);
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Rex', 'Fido']);

    typeFilter(container, 2, '3');
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Rex']);

    // Clear the kind filter
    fireEvent.click(container.querySelector('.ct-thead.-filters .bp6-input-action button')!);
    typeFilter(container, 3, '5');
    expect(onFilteredChange).toHaveBeenLastCalledWith([
      { id: 'age', value: '~3' },
      { id: 'nameLength', value: '~5' },
    ]);
    expect(getRows(container)).toEqual([]);
    expect(container.querySelector('.ct-noData')!.textContent).toEqual('No rows found');

    typeFilter(container, 2, '5');
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Kitty']);
  });

  it('filters on hidden columns but not on unfilterable ones', () => {
    const { container, rerender } = render(
      <ConsoleTable data={PETS} columns={COLUMNS} filtered={[{ id: 'owner', value: '=Bob' }]} />,
    );
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Tom', 'Nemo']);

    rerender(
      <ConsoleTable
        data={PETS}
        columns={COLUMNS.map(c => (c.id === 'owner' ? { ...c, filterable: false } : c))}
        filtered={[{ id: 'owner', value: '=Bob' }]}
      />,
    );
    expect(getRows(container)).toHaveLength(5);
  });

  it('paginates', () => {
    const onPageChange = jest.fn();
    const onPageSizeChange = jest.fn();
    const { container, getByText } = render(
      <ConsoleTable
        data={PETS}
        columns={COLUMNS}
        defaultPageSize={2}
        pageSizeOptions={[2, 4]}
        onPageChange={onPageChange}
        onPageSizeChange={onPageSizeChange}
      />,
    );
    expect(getByText('Showing 1-2 of 5')).toBeTruthy();

    const nextButton = container.querySelectorAll('.pagination-bottom button')[1];
    fireEvent.click(nextButton);
    fireEvent.click(nextButton);
    expect(onPageChange).toHaveBeenLastCalledWith(2);
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Nemo']);
    expect(container.querySelectorAll('.ct-tr.-padRow')).toHaveLength(1);
    expect(getByText('Showing 5-5 of 5')).toBeTruthy();
    expect(nextButton.hasAttribute('disabled')).toBe(true);

    // Sorting goes back to the first page
    fireEvent.click(getHeader(container, 'Name'));
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Fido', 'Kitty']);
    expect(onPageSizeChange).not.toHaveBeenCalled();
  });

  it('works with controlled pagination in manual mode', () => {
    const onSortedChange = jest.fn();
    const onPageChange = jest.fn();
    const { container, getByText } = render(
      <ConsoleTable
        data={PETS.slice(0, 2)}
        columns={COLUMNS}
        manual
        pages={4}
        page={1}
        pageSize={2}
        onPageChange={onPageChange}
        sorted={[{ id: 'name', desc: true }]}
        onSortedChange={onSortedChange}
        filtered={[{ id: 'kind', value: 'fish' }]}
        ofText="of 8"
      />,
    );
    // The data is not sorted or filtered on the client
    expect(getRows(container).map(r => r.split('|')[0])).toEqual(['Rex', 'Tom']);
    expect(getByText('Showing 3-4 of 8')).toBeTruthy();

    fireEvent.click(getHeader(container, 'Name'));
    expect(onSortedChange).toHaveBeenLastCalledWith([{ id: 'name', desc: false }]);
    expect(getHeader(container, 'Name').className).toContain('-sort-desc');

    fireEvent.click(container.querySelectorAll('.pagination-bottom button')[1]);
    expect(onPageChange).toHaveBeenLastCalledWith(2);
  });

  it('groups rows with pivotBy', () => {
    const { container } = render(
      <ConsoleTable
        data={PETS}
        pivotBy={['kind']}
        defaultPageSize={10}
        columns={[
          { Header: 'Name', accessor: 'name', Aggregated: () => '' },
          COLUMNS[1],
          {
            Header: 'Age',
            accessor: 'age',
            Aggregated: ({ subRows }) =>
              `total: ${subRows.reduce((t, r) => t + (r._original.age || 0), 0)}`,
          },
          { Header: 'Owner', id: 'owner', accessor: 'owner.name' },
        ]}
      />,
    );

    // The pivot column goes first
    expect(container.querySelector('.ct-thead.-header .ct-th')!.textContent).toEqual('Kind');
    expect(getRows(container)).toEqual([
      '•dog (2)||total: 3|Ann (1), Cid (1)',
      '•cat (2)||total: 10|Ann (1), Bob (1)',
      '•fish (1)||total: 1|Bob (1)',
    ]);

    fireEvent.click(container.querySelectorAll('.ct-td.ct-pivot')[1]);
    expect(getRows(container)).toEqual([
      '•dog (2)||total: 3|Ann (1), Cid (1)',
      '•cat (2)||total: 10|Ann (1), Bob (1)',
      '|Tom|5|Bob',
      '|Kitty|5|Ann',
      '•fish (1)||total: 1|Bob (1)',
    ]);
    expect(container.querySelector('.ct-td.ct-pivot .ct-expander.-open')).toBeTruthy();

    // Sorting sorts the rows within the groups
    fireEvent.click(getHeader(container, 'Name'));
    expect(getRows(container)).toEqual([
      '•dog (2)||total: 3|Ann (1), Cid (1)',
      '•cat (2)||total: 10|Ann (1), Bob (1)',
      '•fish (1)||total: 1|Bob (1)',
    ]);
    fireEvent.click(container.querySelectorAll('.ct-td.ct-pivot')[0]);
    expect(getRows(container).slice(1, 3)).toEqual(['|Fido||Cid', '|Rex|3|Ann']);
  });

  it('expands rows with a SubComponent', () => {
    const props = {
      data: PETS,
      columns: COLUMNS,
      SubComponent: ({ original }: { original: Pet }) => (
        <div className="sub">{`${original.name} is a ${original.kind}`}</div>
      ),
    };
    const { container, rerender } = render(<ConsoleTable {...props} />);
    expect(container.querySelectorAll('.ct-thead.-header .ct-th')).toHaveLength(4);

    fireEvent.click(container.querySelectorAll('.ct-tbody .ct-td.ct-expandable')[1]);
    expect(container.querySelector('.ct-tr-group:nth-child(2) > .sub')!.textContent).toEqual(
      'Tom is a cat',
    );

    // Collapses when the data changes
    rerender(<ConsoleTable {...props} data={PETS.slice()} />);
    expect(container.querySelector('.sub')).toBeNull();

    // ...unless told otherwise
    rerender(<ConsoleTable {...props} collapseOnDataChange={false} />);
    fireEvent.click(container.querySelectorAll('.ct-tbody .ct-td.ct-expandable')[1]);
    rerender(<ConsoleTable {...props} data={PETS.slice()} collapseOnDataChange={false} />);
    expect(container.querySelector('.sub')!.textContent).toEqual('Tom is a cat');
  });

  it('shows the no data text and the loader', () => {
    const { container, rerender } = render(
      <ConsoleTable data={[]} columns={COLUMNS} noDataText="No pets" loading />,
    );
    expect(container.querySelector('.ct-noData')!.textContent).toEqual('No pets');
    expect(container.querySelector('.loader')).toBeTruthy();

    rerender(<ConsoleTable data={PETS} columns={COLUMNS} />);
    expect(container.querySelector('.ct-noData')).toBeNull();
    expect(container.querySelector('.loader')).toBeNull();
  });
});
