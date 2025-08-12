import useSWR from 'swr';
import { getAPIClient } from '@/lib/api/client';
import { PaginatedResponse, Disclosure, FilterParams } from '@/lib/api/types';

const apiClient = getAPIClient();

export function useDisclosures(params: FilterParams = {}) {
  const key = ['disclosures', params];
  
  const { data, error, mutate } = useSWR<PaginatedResponse<Disclosure>>(
    key,
    () => apiClient.getDisclosures(params),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    }
  );

  return {
    disclosures: data?.items || [],
    total: data?.total || 0,
    pages: data?.pages || 0,
    isLoading: !error && !data,
    isError: error,
    mutate,
  };
}

export function useDisclosure(id: string | null) {
  const { data, error, mutate } = useSWR<Disclosure>(
    id ? ['disclosure', id] : null,
    () => id ? apiClient.getDisclosure(id) : Promise.reject('No ID'),
    {
      revalidateOnFocus: false,
    }
  );

  return {
    disclosure: data,
    isLoading: !error && !data,
    isError: error,
    mutate,
  };
}