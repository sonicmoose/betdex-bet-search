export type MarketStatus = 'Open' | 'Suspended' | 'Settled' | 'Closed';
export type InPlayFilter = 'Yes' | 'No';
export type MarketSort = 'Relevance' | 'Start time' | 'Matched' | 'Liquidity';

export interface MarketSearchInput {
  text: string;
  inPlay: InPlayFilter[];
  subCategoryIds: string[];
  eventGroupIds: string[];
  sort: MarketSort;
  page: number;
  pageSize: number;
}

export interface MarketSearchResult {
  items: Market[];
  total: number;
  page: number;
  pageSize: number;
}

export interface Market {
  marketId: string;
  name: string;
  eventId: string;
  eventGroupId: string;
  eventName: string;
  categoryId: string;
  categoryName: string;
  subCategoryId: string;
  subCategoryName: string;
  status: MarketStatus;
  inPlay: boolean;
  startsAt: string;
  matched: number;
  liquidity: number;
  outcomes: PricePoint[];
  raw: Record<string, unknown>;
}

export interface PricePoint {
  outcomeId: string;
  outcomeName: string;
  side: 'For' | 'Against';
  price: number;
  liquidity: number;
  change: number;
  validAt: string;
}
