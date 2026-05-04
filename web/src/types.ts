export type MarketStatus = 'Initializing' | 'Open' | 'Locked' | 'Suspended' | 'Settling' | 'Settled' | 'Voiding' | 'Voided' | 'Closed';
export type InPlayFilter = 'Yes' | 'No';
export type MarketSort = 'Start time' | 'Liquidity';

export interface MarketSearchInput {
  text: string;
  inPlay: InPlayFilter[];
  subCategoryIds: string[];
  eventGroupIds: string[];
  hasLiquidity: boolean;
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
  marketOutcomes: MarketOutcome[];
  outcomes: PricePoint[];
  raw: Record<string, unknown>;
}

export interface MarketOutcome {
  outcomeId: string;
  outcomeName: string;
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

export interface MarketUpdate {
  marketId: string;
  eventId?: string;
  updateType?: string;
  receivedAt: string;
  source?: Market;
}
