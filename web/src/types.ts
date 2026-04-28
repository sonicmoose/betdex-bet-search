export type MarketStatus = 'Open' | 'Suspended' | 'Settled' | 'Closed';

export interface MarketSearchInput {
  text: string;
  status: 'Any' | MarketStatus;
  inPlay: 'Any' | 'Yes' | 'No';
  categoryId: string;
}

export interface Market {
  marketId: string;
  name: string;
  eventId: string;
  eventName: string;
  categoryId: string;
  categoryName: string;
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

export interface MarketUpdate {
  marketId: string;
  messageType: string;
  receivedAt: string;
  payload: Record<string, unknown>;
}
