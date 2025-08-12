import useSWR from 'swr';
import { getAPIClient } from '@/lib/api/client';
import { PaginatedResponse, Disclosure, FilterParams } from '@/lib/api/types';

const apiClient = getAPIClient();

// Helper function to fetch all pages
async function fetchAllDisclosures(params: FilterParams = {}): Promise<PaginatedResponse<Disclosure>> {
  // First, get the first page to know the total
  const firstPage = await apiClient.getDisclosures({ ...params, page: 1, page_size: 100 });
  
  // If there's only one page, return it
  if (firstPage.pages <= 1) {
    return firstPage;
  }
  
  // Fetch all remaining pages in parallel
  const promises = [];
  for (let page = 2; page <= firstPage.pages; page++) {
    promises.push(apiClient.getDisclosures({ ...params, page, page_size: 100 }));
  }
  
  const remainingPages = await Promise.all(promises);
  
  // Combine all items
  const allItems = [...firstPage.items];
  remainingPages.forEach(page => {
    allItems.push(...page.items);
  });
  
  return {
    items: allItems,
    total: firstPage.total,
    page: 1,
    page_size: allItems.length,
    pages: 1
  };
}

export function useDisclosures(params: FilterParams = {}) {
  const key = ['disclosures', params];
  
  const { data, error, mutate } = useSWR<PaginatedResponse<Disclosure>>(
    key,
    () => {
      // If requesting all data (page_size >= 100), fetch all pages
      if (!params.page_size || params.page_size >= 100) {
        return fetchAllDisclosures(params);
      }
      // Otherwise just fetch the requested page
      return apiClient.getDisclosures(params);
    },
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