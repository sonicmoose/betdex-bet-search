import { markets as mockMarkets, searchMarkets as searchMockMarkets } from './mockApi';
import type { Market, MarketSearchInput, MarketSearchResult, MarketSort, MarketStatus, PricePoint } from './types';

const appsyncUrl = import.meta.env.VITE_APPSYNC_GRAPHQL_URL as string | undefined;
const appsyncApiKey = import.meta.env.VITE_APPSYNC_API_KEY as string | undefined;
const forceMock = import.meta.env.VITE_DATA_SOURCE === 'mock';

export const isLiveDataSource = Boolean(appsyncUrl && appsyncApiKey && !forceMock);
export const dataSourceLabel = isLiveDataSource ? 'Live data' : 'Mock data';
export const initialMarkets = mockMarkets.slice(0, 10);

const searchMarketsQuery = `
  query SearchMarkets($input: MarketSearchInput!) {
    searchMarkets(input: $input) {
      total
      items {
        id
        source
      }
    }
  }
`;

export async function searchMarkets(input: MarketSearchInput): Promise<MarketSearchResult> {
  if (!isLiveDataSource) {
    return searchMockMarkets(input);
  }

  const response = await fetch(appsyncUrl!, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': appsyncApiKey!
    },
    body: JSON.stringify({
      query: searchMarketsQuery,
      variables: {
        input: toGraphqlInput(input)
      }
    })
  });

  if (!response.ok) {
    throw new Error(`AppSync search failed with HTTP ${response.status}`);
  }

  const payload = await response.json() as GraphqlResponse;
  if (payload.errors?.length) {
    throw new Error(payload.errors.map((error) => error.message).join('; '));
  }

  const result = payload.data?.searchMarkets;
  if (!result) {
    throw new Error('AppSync search response did not include searchMarkets');
  }

  return {
    items: result.items.map((hit) => toMarket(hit.id, parseAwsJson(hit.source))),
    total: result.total,
    page: input.page,
    pageSize: input.pageSize
  };
}

function toGraphqlInput(input: MarketSearchInput) {
  return {
    text: input.text.trim() || undefined,
    categoryIds: input.categoryIds.length ? input.categoryIds : undefined,
    subCategoryIds: input.subCategoryIds.length ? input.subCategoryIds : undefined,
    statuses: input.statuses.length ? input.statuses : undefined,
    inPlay: input.inPlay.length ? input.inPlay.map((value) => value === 'Yes') : undefined,
    sort: toGraphqlSort(input.sort),
    page: input.page,
    pageSize: input.pageSize
  };
}

function toGraphqlSort(sort: MarketSort): string {
  switch (sort) {
    case 'Start time':
      return 'START_TIME';
    case 'Matched':
      return 'MATCHED';
    case 'Liquidity':
      return 'LIQUIDITY';
    case 'Relevance':
    default:
      return 'RELEVANCE';
  }
}

function toMarket(hitId: string, raw: Record<string, unknown>): Market {
  const marketId = text(raw, 'marketId') ?? text(raw, 'id') ?? hitId;
  const marketOutcomes = array(raw, 'marketOutcomes');
  const outcomes = marketOutcomes.length > 0 ? marketOutcomes.map(toPricePoint) : [];
  const liquidity = number(raw, 'liquidity') ?? sum(outcomes.map((outcome) => outcome.liquidity));
  const eventName = text(raw, 'eventName') ?? text(raw, 'eventGroupName') ?? text(raw, 'eventId') ?? 'Unknown event';

  return {
    marketId,
    name: text(raw, 'name') ?? text(raw, 'marketName') ?? 'Market',
    eventId: text(raw, 'eventId') ?? '',
    eventName,
    categoryId: text(raw, 'categoryId') ?? '',
    categoryName: text(raw, 'categoryName') ?? text(raw, 'categoryId') ?? 'Unknown sport',
    subCategoryId: text(raw, 'subCategoryId') ?? '',
    subCategoryName: text(raw, 'subCategoryName') ?? text(raw, 'subCategoryId') ?? 'Unknown league',
    status: normalizeStatus(text(raw, 'status')),
    inPlay: text(raw, 'inPlayStatus') === 'InPlay' || boolean(raw, 'inPlay'),
    startsAt: text(raw, 'lockAt') ?? text(raw, 'expectedStartTime') ?? text(raw, 'startsAt') ?? new Date().toISOString(),
    matched: number(raw, 'matched') ?? number(raw, 'totalMatched') ?? 0,
    liquidity,
    outcomes,
    raw
  };
}

function toPricePoint(raw: Record<string, unknown>, index: number): PricePoint {
  const outcomeId = text(raw, 'outcomeId') ?? text(raw, 'id') ?? `outcome-${index}`;

  return {
    outcomeId,
    outcomeName: text(raw, 'name') ?? text(raw, 'outcomeName') ?? outcomeId,
    side: text(raw, 'side') === 'Against' ? 'Against' : 'For',
    price: number(raw, 'price') ?? number(raw, 'odds') ?? 0,
    liquidity: number(raw, 'liquidity') ?? 0,
    change: number(raw, 'change') ?? 0,
    validAt: text(raw, 'validAt') ?? new Date().toISOString()
  };
}

function parseAwsJson(value: unknown): Record<string, unknown> {
  if (typeof value === 'string') {
    const parsed = JSON.parse(value) as unknown;
    return isRecord(parsed) ? parsed : {};
  }
  return isRecord(value) ? value : {};
}

function normalizeStatus(value: string | undefined): MarketStatus {
  const normalized = value?.toLowerCase();
  if (normalized === 'open') {
    return 'Open';
  }
  if (normalized === 'suspended') {
    return 'Suspended';
  }
  if (normalized === 'settled') {
    return 'Settled';
  }
  if (normalized === 'closed') {
    return 'Closed';
  }
  return 'Open';
}

function text(source: Record<string, unknown>, key: string): string | undefined {
  const value = source[key];
  return typeof value === 'string' && value.length > 0 ? value : undefined;
}

function number(source: Record<string, unknown>, key: string): number | undefined {
  const value = source[key];
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'string' && value.length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : undefined;
  }
  return undefined;
}

function boolean(source: Record<string, unknown>, key: string): boolean {
  return source[key] === true;
}

function array(source: Record<string, unknown>, key: string): Array<Record<string, unknown>> {
  const value = source[key];
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function sum(values: number[]): number {
  return values.reduce((total, value) => total + value, 0);
}

interface GraphqlResponse {
  data?: {
    searchMarkets?: {
      total: number;
      items: Array<{
        id: string;
        source: unknown;
      }>;
    };
  };
  errors?: Array<{
    message: string;
  }>;
}
