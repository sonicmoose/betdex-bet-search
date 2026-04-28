import type { Market, MarketSearchInput, MarketUpdate } from './types';

const now = Date.now();

export const markets: Market[] = [
  {
    marketId: 'mkt-ars-che-ml',
    name: 'Match Winner',
    eventId: 'evt-ars-che',
    eventName: 'Arsenal v Chelsea',
    categoryId: 'football',
    categoryName: 'Football',
    subCategoryName: 'Premier League',
    status: 'Open',
    inPlay: true,
    startsAt: new Date(now - 28 * 60_000).toISOString(),
    matched: 183420.42,
    liquidity: 62418.15,
    outcomes: [
      { outcomeId: 'arsenal', outcomeName: 'Arsenal', side: 'For', price: 2.14, liquidity: 18232.12, change: 0.08, validAt: new Date(now - 9_000).toISOString() },
      { outcomeId: 'draw', outcomeName: 'Draw', side: 'For', price: 3.35, liquidity: 11300.54, change: -0.02, validAt: new Date(now - 9_000).toISOString() },
      { outcomeId: 'chelsea', outcomeName: 'Chelsea', side: 'For', price: 3.8, liquidity: 14887.33, change: -0.14, validAt: new Date(now - 9_000).toISOString() }
    ],
    raw: { marketType: 'MATCH_ODDS', currencyId: 'USDC', inPlayStatus: 'InPlay' }
  },
  {
    marketId: 'mkt-celtics-knicks-spread',
    name: 'Point Spread',
    eventId: 'evt-celtics-knicks',
    eventName: 'Boston Celtics v New York Knicks',
    categoryId: 'basketball',
    categoryName: 'Basketball',
    subCategoryName: 'NBA',
    status: 'Open',
    inPlay: false,
    startsAt: new Date(now + 5 * 60 * 60_000).toISOString(),
    matched: 52620.11,
    liquidity: 22014.64,
    outcomes: [
      { outcomeId: 'bos-minus-4', outcomeName: 'Boston -4.5', side: 'For', price: 1.91, liquidity: 7340.22, change: 0.01, validAt: new Date(now - 32_000).toISOString() },
      { outcomeId: 'nyk-plus-4', outcomeName: 'New York +4.5', side: 'For', price: 1.95, liquidity: 6910.7, change: -0.01, validAt: new Date(now - 32_000).toISOString() }
    ],
    raw: { marketType: 'HANDICAP', currencyId: 'USDC', inPlayStatus: 'PrePlay' }
  },
  {
    marketId: 'mkt-miami-open-final',
    name: 'Tournament Winner',
    eventId: 'evt-miami-open',
    eventName: 'Miami Open Final',
    categoryId: 'tennis',
    categoryName: 'Tennis',
    subCategoryName: 'ATP',
    status: 'Suspended',
    inPlay: true,
    startsAt: new Date(now - 2 * 60 * 60_000).toISOString(),
    matched: 94310.9,
    liquidity: 12004.08,
    outcomes: [
      { outcomeId: 'player-a', outcomeName: 'Player A', side: 'For', price: 1.42, liquidity: 4490.5, change: -0.18, validAt: new Date(now - 70_000).toISOString() },
      { outcomeId: 'player-b', outcomeName: 'Player B', side: 'For', price: 3.15, liquidity: 6122.8, change: 0.32, validAt: new Date(now - 70_000).toISOString() }
    ],
    raw: { marketType: 'OUTRIGHT', currencyId: 'USDC', inPlayStatus: 'InPlay' }
  },
  {
    marketId: 'mkt-giants-dodgers-total',
    name: 'Total Runs',
    eventId: 'evt-giants-dodgers',
    eventName: 'San Francisco Giants v Los Angeles Dodgers',
    categoryId: 'baseball',
    categoryName: 'Baseball',
    subCategoryName: 'MLB',
    status: 'Open',
    inPlay: false,
    startsAt: new Date(now + 24 * 60 * 60_000).toISOString(),
    matched: 31220.18,
    liquidity: 18550.25,
    outcomes: [
      { outcomeId: 'over-8', outcomeName: 'Over 8.5', side: 'For', price: 2.02, liquidity: 6230.99, change: 0.03, validAt: new Date(now - 17_000).toISOString() },
      { outcomeId: 'under-8', outcomeName: 'Under 8.5', side: 'For', price: 1.88, liquidity: 5840.02, change: -0.03, validAt: new Date(now - 17_000).toISOString() }
    ],
    raw: { marketType: 'TOTAL', currencyId: 'USDC', inPlayStatus: 'PrePlay' }
  }
];

const updates: MarketUpdate[] = markets.flatMap((market, index) => [
  {
    marketId: market.marketId,
    messageType: 'MarketUpdate',
    receivedAt: new Date(now - (index + 3) * 90_000).toISOString(),
    payload: { marketId: market.marketId, status: market.status, name: market.name }
  },
  {
    marketId: market.marketId,
    messageType: 'MarketPriceUpdate',
    receivedAt: new Date(now - (index + 1) * 30_000).toISOString(),
    payload: { marketId: market.marketId, updateType: 'Incremental', prices: market.outcomes }
  }
]);

export async function searchMarkets(input: MarketSearchInput): Promise<Market[]> {
  await delay(180);
  const text = input.text.trim().toLowerCase();
  return markets.filter((market) => {
    const textMatch = !text || [market.name, market.eventName, market.categoryName, market.subCategoryName]
      .some((value) => value.toLowerCase().includes(text));
    const statusMatch = input.status === 'Any' || market.status === input.status;
    const inPlayMatch = input.inPlay === 'Any' || market.inPlay === (input.inPlay === 'Yes');
    const categoryMatch = input.categoryId === 'Any' || market.categoryId === input.categoryId;
    return textMatch && statusMatch && inPlayMatch && categoryMatch;
  });
}

export async function getMarketUpdates(marketId: string): Promise<MarketUpdate[]> {
  await delay(120);
  return updates.filter((update) => update.marketId === marketId);
}

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
