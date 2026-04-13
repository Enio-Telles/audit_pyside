import { renderHook, waitFor } from '@testing-library/react';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { ReactNode } from 'react';
import { useRelatorio } from '../useRelatorio';
import api from '../../api/client';
import { vi, describe, it, expect, beforeEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('../../api/client', () => {
  return {
    default: {
      get: vi.fn(),
    },
  };
});

describe('useRelatorio', () => {
  let queryClient: QueryClient;

  beforeEach(() => {
    queryClient = new QueryClient({
      defaultOptions: {
        queries: {
          retry: false,
        },
      },
    });
    vi.clearAllMocks();
  });

  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );

  it('should fetch report data successfully', async () => {
    const mockData = {
      id: '123',
      title: 'Test Report',
      content: 'This is a test report content',
      createdAt: '2023-10-27T10:00:00Z',
    };

    (api.get as Mock).mockResolvedValueOnce({ data: mockData });

    const { result } = renderHook(() => useRelatorio('123'), { wrapper });

    expect(result.current.isLoading).toBe(true);

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    expect(result.current.data).toEqual(mockData);
    expect(api.get).toHaveBeenCalledWith('/relatorios/123');
    expect(api.get).toHaveBeenCalledTimes(1);
  });

  it('should not fetch if id is empty', () => {
    const { result } = renderHook(() => useRelatorio(''), { wrapper });

    expect(result.current.isPending).toBe(true);
    expect(result.current.fetchStatus).toBe('idle');
    expect(api.get).not.toHaveBeenCalled();
  });

  it('should handle API errors', async () => {
    const error = new Error('Network Error');
    (api.get as Mock).mockRejectedValueOnce(error);

    const { result } = renderHook(() => useRelatorio('123'), { wrapper });

    await waitFor(() => {
      expect(result.current.isError).toBe(true);
    });

    expect(result.current.error).toEqual(error);
  });
});
