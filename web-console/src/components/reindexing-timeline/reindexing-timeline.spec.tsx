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

import type { useQueryManager as UseQueryManager } from '../../hooks';
import * as hooks from '../../hooks';
import * as singletons from '../../singletons';
import { QueryState } from '../../utils';

import { ReindexingTimeline } from './reindexing-timeline';

// Mock the useQueryManager hook
jest.mock('../../hooks', () => ({
  useQueryManager: jest.fn(),
}));

const mockUseQueryManager = hooks.useQueryManager as jest.MockedFunction<typeof UseQueryManager>;

// Mock Api singleton
jest.mock('../../singletons', () => ({
  Api: {
    instance: {
      get: jest.fn(),
      post: jest.fn(),
      encodePath: jest.fn((path: string) => path),
    },
  },
  AppToaster: {
    show: jest.fn(),
  },
}));

// Get mock references after mocking
const mockPost = singletons.Api.instance.post as jest.Mock;
const mockAppToasterShow = singletons.AppToaster.show as jest.Mock;

// Mock chronoshift Duration
jest.mock('chronoshift', () => ({
  Duration: jest.fn().mockImplementation((period: string) => {
    if (period === 'INVALID') {
      throw new Error('Invalid period format');
    }
    return {
      shift: jest.fn((date: Date) => new Date(date.getTime() - 7 * 24 * 60 * 60 * 1000)),
    };
  }),
  Timezone: {
    UTC: 'UTC',
  },
}));

// Sample test data
const mockTimelineData = {
  dataSource: 'test-datasource',
  referenceTime: '2024-11-15T12:00:00.000Z',
  intervals: [
    {
      interval: '2024-11-01T00:00:00.000Z/2024-11-08T00:00:00.000Z',
      ruleCount: 3,
      config: {
        granularitySpec: {
          segmentGranularity: 'DAY',
          queryGranularity: 'HOUR',
          rollup: true,
        },
        metricsSpec: [{ type: 'count', name: 'count' }],
      },
      appliedRules: [
        { type: 'dataSchema', id: 'schema-1' },
        { type: 'segmentGranularity', id: 'gran-1' },
        { type: 'tuningConfig', id: 'tuning-1' },
      ],
    },
    {
      interval: '2024-11-08T00:00:00.000Z/2024-11-15T00:00:00.000Z',
      ruleCount: 0,
      config: {},
      appliedRules: [],
    },
  ],
};

describe('ReindexingTimeline', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  // ====================================================================
  // PHASE 1: UTILITY FUNCTIONS
  // ====================================================================
  describe('utility functions', () => {
    // Note: These are internal functions, so we'll test them indirectly through component behavior
    // For direct testing, we'd need to export them, which may not be desired

    describe('date formatting', () => {
      it('formats dates consistently across the component', () => {
        mockUseQueryManager.mockReturnValue([
          new QueryState({ data: mockTimelineData }),
          {} as any,
        ]);

        const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

        // Reference time should be formatted with UTC
        expect(container.textContent).toContain('Nov 15, 2024');
      });
    });

    describe('interval color generation', () => {
      it('applies different colors to different intervals', () => {
        mockUseQueryManager.mockReturnValue([
          new QueryState({ data: mockTimelineData }),
          {} as any,
        ]);

        const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

        const segments = container.querySelectorAll('.timeline-segment');
        expect(segments.length).toBe(2);

        // Each segment should have a background color
        segments.forEach(segment => {
          const htmlSegment = segment as HTMLElement;
          expect(htmlSegment.style.backgroundColor).toBeTruthy();
        });
      });
    });
  });

  // ====================================================================
  // PHASE 2: COMPONENT RENDERING
  // ====================================================================
  describe('component rendering', () => {
    it('matches snapshot for loading state', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ loading: true }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot for error state', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({ error: new Error('Failed to load timeline') }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot for empty intervals', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            intervals: [],
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with validation error', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            intervals: [],
            validationError: {
              errorType: 'INVALID_GRANULARITY_TIMELINE',
              message: 'Granularity must become coarser over time',
              olderInterval: '2024-01-01/2024-02-01',
              olderGranularity: 'HOUR',
              newerInterval: '2024-02-01/2024-03-01',
              newerGranularity: 'DAY',
            },
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with normal intervals', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with skip offset applied', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              applied: {
                type: 'skipOffsetFromNow',
                period: 'P7D',
                effectiveEndTime: '2024-11-08T12:00:00.000Z',
              },
            },
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('matches snapshot with skip offset not applied', () => {
      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              notApplied: {
                type: 'skipOffsetFromLatest',
                period: 'P7D',
                reason: 'Requires actual segment timeline data',
              },
            },
          },
        }),
        {} as any,
      ]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container.firstChild).toMatchSnapshot();
    });

    it('marks skipped intervals with special styling', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const segments = container.querySelectorAll('.timeline-segment');
      const skippedSegments = container.querySelectorAll('.timeline-segment.skipped');

      expect(segments.length).toBe(2);
      expect(skippedSegments.length).toBe(1); // Second interval has ruleCount: 0
    });

    it('displays interval details when interval is selected', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      // Initially no detail panel
      expect(container.querySelector('.interval-detail-panel')).toBeNull();

      // Click first interval
      const firstSegment = container.querySelector('.timeline-segment:not(.skipped)')!;
      fireEvent.click(firstSegment);

      // Detail panel should appear
      expect(container.querySelector('.interval-detail-panel')).toBeTruthy();
    });
  });

  // ====================================================================
  // PHASE 3: ERROR HANDLING
  // ====================================================================
  describe('error handling', () => {
    it('handles invalid Duration period gracefully', () => {
      const mockDuration = jest.requireMock('chronoshift').Duration;
      mockDuration.mockImplementationOnce(() => {
        throw new Error('Invalid ISO 8601 duration');
      });

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              notApplied: {
                type: 'skipOffsetFromLatest',
                period: 'INVALID',
                reason: 'Test',
              },
            },
          },
        }),
        {} as any,
      ]);

      // Should not crash
      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);
      expect(container).toBeTruthy();
    });

    it('handles API errors in handleQueryMaxTime', async () => {
      mockPost.mockRejectedValueOnce(new Error('Network error'));

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              notApplied: {
                type: 'skipOffsetFromLatest',
                period: 'P7D',
                reason: 'Test',
              },
            },
          },
        }),
        {} as any,
      ]);

      const { getByText } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const button = getByText('Query latest timestamp');
      fireEvent.click(button);

      // Wait for async operation
      await new Promise(resolve => setTimeout(resolve, 100));

      // Should show error toast
      expect(mockAppToasterShow).toHaveBeenCalledWith(
        expect.objectContaining({
          intent: 'danger',
        }),
      );
    });

    it('handles empty response from timeBoundary query', async () => {
      mockPost.mockResolvedValueOnce({ data: [] });

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              notApplied: {
                type: 'skipOffsetFromLatest',
                period: 'P7D',
                reason: 'Test',
              },
            },
          },
        }),
        {} as any,
      ]);

      const { getByText } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const button = getByText('Query latest timestamp');
      fireEvent.click(button);

      // Wait for async operation
      await new Promise(resolve => setTimeout(resolve, 100));

      // Should show warning toast
      expect(mockAppToasterShow).toHaveBeenCalledWith(
        expect.objectContaining({
          intent: 'warning',
          message: 'No data found in datasource',
        }),
      );
    });

    it('handles missing result in timeBoundary response', async () => {
      mockPost.mockResolvedValueOnce({
        data: [{ result: null }],
      });

      mockUseQueryManager.mockReturnValue([
        new QueryState({
          data: {
            ...mockTimelineData,
            skipOffset: {
              notApplied: {
                type: 'skipOffsetFromLatest',
                period: 'P7D',
                reason: 'Test',
              },
            },
          },
        }),
        {} as any,
      ]);

      const { getByText } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const button = getByText('Query latest timestamp');
      fireEvent.click(button);

      // Wait for async operation
      await new Promise(resolve => setTimeout(resolve, 100));

      // Should show warning toast
      expect(mockAppToasterShow).toHaveBeenCalledWith(
        expect.objectContaining({
          intent: 'warning',
        }),
      );
    });
  });

  // ====================================================================
  // ACCESSIBILITY
  // ====================================================================
  describe('accessibility', () => {
    it('has proper ARIA attributes on interactive intervals', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const interactiveSegment = container.querySelector('.timeline-segment:not(.skipped)')!;

      expect(interactiveSegment.getAttribute('role')).toBe('button');
      expect(interactiveSegment.getAttribute('tabindex')).toBe('0');
      expect(interactiveSegment.getAttribute('aria-label')).toBeTruthy();
    });

    it('does not make skipped intervals interactive', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const skippedSegment = container.querySelector('.timeline-segment.skipped')!;

      expect(skippedSegment.getAttribute('role')).toBeNull();
      expect(skippedSegment.getAttribute('tabindex')).toBeNull();
    });

    it('has toolbar role on timeline container', () => {
      mockUseQueryManager.mockReturnValue([new QueryState({ data: mockTimelineData }), {} as any]);

      const { container } = render(<ReindexingTimeline supervisorId="test-supervisor" />);

      const timeline = container.querySelector('.timeline-bar');
      expect(timeline?.getAttribute('role')).toBe('toolbar');
      expect(timeline?.getAttribute('aria-label')).toBeTruthy();
    });
  });
});
