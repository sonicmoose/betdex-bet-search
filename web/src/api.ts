import { markets as mockMarkets, searchMarkets as searchMockMarkets } from './mockApi';
import type { Market, MarketOutcome, MarketSearchInput, MarketSearchResult, MarketSort, MarketStatus, MarketUpdate, PricePoint } from './types';

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

const marketUpdatedSubscription = `
  subscription OnMarketUpdated($marketId: ID) {
    onMarketUpdated(marketId: $marketId) {
      marketId
      eventId
      updateType
      receivedAt
      source
    }
  }
`;

export async function searchMarkets(input: MarketSearchInput, signal?: AbortSignal): Promise<MarketSearchResult> {
  if (!isLiveDataSource) {
    return searchMockMarkets(input);
  }

  const response = await fetch(appsyncUrl!, {
    method: 'POST',
    signal,
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

export function subscribeToMarketUpdates(
  marketIds: string[],
  onUpdate: (update: MarketUpdate) => void,
  onError: (reason: Error) => void
): () => void {
  const ids = Array.from(new Set(marketIds.filter(Boolean)));
  if (!isLiveDataSource || ids.length === 0) {
    return () => {};
  }

  const socket = new WebSocket(appsyncRealtimeUrl(), 'graphql-ws');
  let closed = false;

  socket.addEventListener('open', () => {
    socket.send(JSON.stringify({ type: 'connection_init' }));
  });
  socket.addEventListener('message', (event) => {
    const message = JSON.parse(event.data as string) as AppSyncRealtimeMessage;
    if (message.type === 'connection_ack') {
      ids.forEach((marketId) => {
        socket.send(JSON.stringify({
          id: marketId,
          type: 'start',
          payload: {
            data: JSON.stringify({
              query: marketUpdatedSubscription,
              variables: { marketId }
            }),
            extensions: {
              authorization: appsyncAuthorization()
            }
          }
        }));
      });
      return;
    }
    if (message.type === 'data') {
      const update = message.payload?.data?.onMarketUpdated;
      if (update) {
        onUpdate(toMarketUpdate(update));
      }
      return;
    }
    if (message.type === 'error') {
      onError(new Error(JSON.stringify(message.payload ?? message)));
    }
  });
  socket.addEventListener('error', () => {
    if (!closed) {
      onError(new Error('AppSync realtime connection failed'));
    }
  });

  return () => {
    closed = true;
    ids.forEach((marketId) => {
      if (socket.readyState === WebSocket.OPEN) {
        socket.send(JSON.stringify({ id: marketId, type: 'stop' }));
      }
    });
    socket.close();
  };
}

function toGraphqlInput(input: MarketSearchInput) {
  return {
    text: input.text.trim() || undefined,
    subCategoryIds: input.subCategoryIds.length ? input.subCategoryIds : undefined,
    eventGroupId: input.eventGroupIds.length === 1 ? input.eventGroupIds[0] : undefined,
    eventGroupIds: input.eventGroupIds.length > 1 ? input.eventGroupIds : undefined,
    status: 'Open',
    hasLiquidity: input.hasLiquidity,
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
    case 'Liquidity':
      return 'LIQUIDITY';
    default:
      return 'START_TIME';
  }
}

function toMarket(hitId: string, raw: Record<string, unknown>): Market {
  const source = normalizeSource(raw);
  const marketId = text(source, 'marketId') ?? text(source, 'id') ?? hitId;
  const marketOutcomes = array(source, 'marketOutcomes');
  const latestPrices = array(source, 'latestPrices');
  const pricedMarketOutcomes = marketOutcomes.filter(hasPrice);
  const outcomeNameLookup = outcomeNames(source);
  const marketOutcomeOptions = toMarketOutcomes(marketOutcomes, outcomeNameLookup);
  const outcomes = (pricedMarketOutcomes.length > 0 ? pricedMarketOutcomes : latestPrices)
    .map((price, index) => toPricePoint(price, index, outcomeNameLookup));
  const liquidity = number(source, 'liquidity') ?? sum(outcomes.map((outcome) => outcome.liquidity));
  const event = record(source, 'event');
  const eventName = text(source, 'eventName') ?? text(source, 'event_name') ?? text(event, 'name') ?? text(source, 'eventGroupName') ?? text(source, 'eventId') ?? 'Unknown event';

  return {
    marketId,
    name: text(source, 'name') ?? text(source, 'marketName') ?? 'Market',
    eventId: text(source, 'eventId') ?? text(source, 'event_id') ?? '',
    eventGroupId: text(source, 'eventGroupId') ?? text(source, 'eventGroup_id') ?? '',
    eventName,
    categoryId: text(source, 'categoryId') ?? text(source, 'category_id') ?? '',
    categoryName: text(source, 'categoryName') ?? text(source, 'category_name') ?? text(source, 'categoryId') ?? text(source, 'category_id') ?? 'Unknown sport',
    subCategoryId: text(source, 'subCategoryId') ?? text(source, 'subCategory_id') ?? '',
    subCategoryName: text(source, 'subCategoryName') ?? text(source, 'subCategory_name') ?? text(source, 'subCategoryId') ?? text(source, 'subCategory_id') ?? 'Unknown league',
    status: normalizeStatus(text(source, 'status')),
    inPlay: text(source, 'inPlayStatus') === 'InPlay' || boolean(source, 'inPlay'),
    startsAt: text(source, 'lockAt') ?? text(source, 'expectedStartTime') ?? text(source, 'startsAt') ?? new Date().toISOString(),
    matched: number(source, 'matched') ?? number(source, 'totalMatched') ?? 0,
    liquidity,
    marketOutcomes: marketOutcomeOptions,
    outcomes,
    raw: source
  };
}

function toMarketUpdate(raw: RawMarketUpdate): MarketUpdate {
  const source = raw.source === undefined ? undefined : toMarket(raw.marketId, parseAwsJson(raw.source));
  return {
    marketId: raw.marketId,
    eventId: raw.eventId,
    updateType: raw.updateType,
    receivedAt: raw.receivedAt,
    source
  };
}

function toPricePoint(raw: Record<string, unknown>, index: number, outcomeNameLookup: Map<string, string>): PricePoint {
  const outcomeId = text(raw, 'outcomeId') ?? text(raw, 'id') ?? `outcome-${index}`;
  const rawName = text(raw, 'name') ?? text(raw, 'outcomeName');

  return {
    outcomeId,
    outcomeName: outcomeNameLookup.get(outcomeId) ?? rawName ?? outcomeId,
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

function appsyncAuthorization() {
  return {
    host: new URL(appsyncUrl!).host,
    'x-api-key': appsyncApiKey!
  };
}

function appsyncRealtimeUrl() {
  const url = new URL(appsyncUrl!);
  url.protocol = 'wss:';
  url.hostname = url.hostname.replace('.appsync-api.', '.appsync-realtime-api.');
  url.searchParams.set('header', base64Url(JSON.stringify(appsyncAuthorization())));
  url.searchParams.set('payload', base64Url('{}'));
  return url.toString();
}

function base64Url(value: string) {
  return btoa(value).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function normalizeSource(source: Record<string, unknown>): Record<string, unknown> {
  const raw = source.raw;
  if (!isRecord(raw)) {
    return source;
  }

  return {
    ...raw,
    ...source,
    latestPrices: source.latestPrices,
    raw
  };
}

function normalizeStatus(value: string | undefined): MarketStatus {
  const normalized = value?.toLowerCase();
  if (normalized === 'initializing') {
    return 'Initializing';
  }
  if (normalized === 'open') {
    return 'Open';
  }
  if (normalized === 'locked') {
    return 'Locked';
  }
  if (normalized === 'suspended') {
    return 'Suspended';
  }
  if (normalized === 'settling') {
    return 'Settling';
  }
  if (normalized === 'settled') {
    return 'Settled';
  }
  if (normalized === 'voiding') {
    return 'Voiding';
  }
  if (normalized === 'voided') {
    return 'Voided';
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

function record(source: Record<string, unknown>, key: string): Record<string, unknown> {
  const value = source[key];
  return isRecord(value) ? value : {};
}

function stringMap(source: Record<string, unknown>, key: string): Map<string, string> {
  const value = source[key];
  if (!isRecord(value)) {
    return new Map();
  }
  return new Map(Object.entries(value)
    .filter((entry): entry is [string, string] => typeof entry[1] === 'string' && entry[1].length > 0));
}

function outcomeNames(source: Record<string, unknown>): Map<string, string> {
  const byId = stringMap(source, 'outcomeNames');
  for (const item of array(source, 'outcomeNameItems')) {
    const outcomeId = text(item, 'outcomeId');
    const name = text(item, 'name');
    if (outcomeId && name) {
      byId.set(outcomeId, name);
    }
  }
  return byId;
}

function toMarketOutcomes(items: Array<Record<string, unknown>>, fallbackNames: Map<string, string>): MarketOutcome[] {
  const byId = new Map<string, MarketOutcome>();
  for (const item of items) {
    const outcomeId = text(item, 'outcomeId') ?? text(item, 'id');
    const outcomeName = text(item, 'outcomeName') ?? text(item, 'name') ?? (outcomeId ? fallbackNames.get(outcomeId) : undefined);
    if (outcomeId && outcomeName) {
      byId.set(outcomeId, { outcomeId, outcomeName });
    }
  }
  for (const [outcomeId, outcomeName] of fallbackNames) {
    if (!byId.has(outcomeId)) {
      byId.set(outcomeId, { outcomeId, outcomeName });
    }
  }
  return Array.from(byId.values());
}

function hasPrice(source: Record<string, unknown>): boolean {
  return number(source, 'price') !== undefined || number(source, 'odds') !== undefined;
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

interface RawMarketUpdate {
  marketId: string;
  eventId?: string;
  updateType?: string;
  receivedAt: string;
  source?: unknown;
}

interface AppSyncRealtimeMessage {
  type: string;
  payload?: {
    data?: {
      onMarketUpdated?: RawMarketUpdate;
    };
  };
}
