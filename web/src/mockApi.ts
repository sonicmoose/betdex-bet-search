import type { Market, MarketOutcome, MarketSearchInput, MarketSearchResult, MarketStatus, PricePoint } from './types';
import { expandMarketTypeGroups, expandSportGroups } from './filterMetadata';

const now = Date.now();

interface MarketSeed {
  categoryId: string;
  categoryName: string;
  eventGroupId?: string;
  subCategoryId?: string;
  subCategoryName: string;
  eventName: string;
  marketName: string;
  inPlay?: boolean;
  status?: MarketStatus;
  startsInHours: number;
}

const seeds: MarketSeed[] = [
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EPL', subCategoryName: 'FOOTBALL', eventName: 'Arsenal v Chelsea', marketName: 'Match Winner', inPlay: true, startsInHours: -0.5 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EPL', subCategoryName: 'FOOTBALL', eventName: 'Liverpool v Manchester City', marketName: 'Total Goals', startsInHours: 20 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'LALIGA', subCategoryName: 'FOOTBALL', eventName: 'Barcelona v Real Madrid', marketName: 'Match Winner', startsInHours: 28 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'FIFAWC', subCategoryName: 'FOOTBALL', eventName: 'Bayern Munich v Inter Milan', marketName: 'Both Teams To Score', startsInHours: 52 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'San Francisco Giants v Los Angeles Dodgers', marketName: 'Total Runs', startsInHours: 24 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'New York Yankees v Boston Red Sox', marketName: 'Moneyline', startsInHours: 6 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'Chicago Cubs v St Louis Cardinals', marketName: 'Run Line', startsInHours: 30 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'Atlanta Braves v New York Mets', marketName: 'Total Runs', inPlay: true, startsInHours: -1 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'Houston Astros v Seattle Mariners', marketName: 'Moneyline', startsInHours: 12 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'Texas Rangers v Oakland Athletics', marketName: 'First Five Innings', startsInHours: 18 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EFL', subCategoryName: 'FOOTBALL', eventName: 'Kansas City Chiefs v Las Vegas Raiders', marketName: 'Moneyline', startsInHours: 72 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EFL', subCategoryName: 'FOOTBALL', eventName: 'Dallas Cowboys v Philadelphia Eagles', marketName: 'Point Spread', startsInHours: 80 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EFL', subCategoryName: 'FOOTBALL', eventName: 'Buffalo Bills v Miami Dolphins', marketName: 'Total Points', startsInHours: 88 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EFL', subCategoryName: 'FOOTBALL', eventName: 'Green Bay Packers v Chicago Bears', marketName: 'Moneyline', startsInHours: 96 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EFL', subCategoryName: 'FOOTBALL', eventName: 'San Francisco 49ers v Seattle Seahawks', marketName: 'Point Spread', status: 'Suspended', startsInHours: 104 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'ICEHKY', eventGroupId: 'NHL', subCategoryName: 'ICEHKY', eventName: 'Toronto Maple Leafs v Boston Bruins', marketName: 'Moneyline', startsInHours: 7 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'ICEHKY', eventGroupId: 'NHL', subCategoryName: 'ICEHKY', eventName: 'New York Rangers v New Jersey Devils', marketName: 'Total Goals', startsInHours: 11 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'CRICKET', eventGroupId: 'IPL', subCategoryName: 'CRICKET', eventName: 'Mumbai Indians v Chennai Super Kings', marketName: 'Match Winner', startsInHours: 16 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'MLB', eventGroupId: 'MLB', subCategoryName: 'MLB', eventName: 'Philadelphia Phillies v Washington Nationals', marketName: 'Moneyline', status: 'Closed', startsInHours: -26 },
  { categoryId: 'SPORTS', categoryName: 'SPORTS', subCategoryId: 'FOOTBALL', eventGroupId: 'EFL', subCategoryName: 'FOOTBALL', eventName: 'Detroit Lions v Minnesota Vikings', marketName: 'Total Points', status: 'Settled', startsInHours: -48 }
];

export const markets: Market[] = seeds.map(createMarket);

export async function searchMarkets(input: MarketSearchInput): Promise<MarketSearchResult> {
  await delay(180);
  const text = input.text.trim().toLowerCase();
  const filtered = markets.filter((market) => {
    const searchable = [
      market.name,
      market.eventName,
      market.categoryName,
      market.subCategoryId,
      market.subCategoryName,
      market.eventGroupId,
      ...market.outcomes.map((outcome) => outcome.outcomeName)
    ];
    const textMatch = !text || searchable.some((value) => value.toLowerCase().includes(text));
    const statusMatch = market.status === 'Open';
    const inPlayMatch = input.inPlay.length === 0 || input.inPlay.includes(market.inPlay ? 'Yes' : 'No');
    const subCategoryIds = expandSportGroups(input.subCategoryIds);
    const sportMatch = subCategoryIds.length === 0 || subCategoryIds.includes(market.subCategoryId);
    const leagueMatch = input.eventGroupIds.length === 0 || input.eventGroupIds.includes(market.eventGroupId);
    const marketTypeIds = expandMarketTypeGroups(input.marketTypeIds);
    const marketTypeMatch = marketTypeIds.length === 0 || marketTypeIds.includes(market.marketTypeId);
    const liquidityMatch = !input.hasLiquidity || market.liquidity > 0;
    return textMatch && statusMatch && inPlayMatch && sportMatch && leagueMatch && marketTypeMatch && liquidityMatch;
  });

  const sorted = [...filtered].sort((left, right) => {
    switch (input.sort) {
      case 'Start time':
        return new Date(left.startsAt).getTime() - new Date(right.startsAt).getTime();
      case 'Liquidity':
        return right.liquidity - left.liquidity;
      default:
        return new Date(left.startsAt).getTime() - new Date(right.startsAt).getTime();
    }
  });

  const pageSize = Math.max(1, input.pageSize);
  const pageCount = Math.max(1, Math.ceil(sorted.length / pageSize));
  const page = Math.min(Math.max(1, input.page), pageCount);
  const start = (page - 1) * pageSize;

  return {
    items: sorted.slice(start, start + pageSize),
    total: sorted.length,
    page,
    pageSize
  };
}

function createMarket(seed: MarketSeed, index: number): Market {
  const startsAt = new Date(now + seed.startsInHours * 60 * 60_000);
  const marketId = `mkt-${slug(seed.eventName)}-${slug(seed.marketName)}`;
  const matched = 18000 + index * 7350 + (index % 5) * 2200;
  const liquidity = 6200 + index * 980 + (index % 4) * 1650;
  const outcomes = createOutcomes(seed, index);
  const marketTypeId = marketTypeIdForSeed(seed);

  return {
    marketId,
    name: seed.marketName,
    eventId: `evt-${slug(seed.eventName)}`,
    eventGroupId: seed.eventGroupId ?? seed.subCategoryName,
    eventName: seed.eventName,
    marketTypeId,
    categoryId: seed.categoryId,
    categoryName: seed.categoryName,
    subCategoryId: seed.subCategoryId ?? seed.subCategoryName,
    subCategoryName: seed.subCategoryName,
    status: seed.status ?? 'Open',
    inPlay: seed.inPlay ?? false,
    startsAt: startsAt.toISOString(),
    matched,
    liquidity,
    marketOutcomes: toMarketOutcomes(outcomes),
    outcomes: outcomes.flatMap(withLayPrice),
    raw: {
      marketTypeId,
      currencyId: 'USDC',
      inPlayStatus: seed.inPlay ? 'InPlay' : 'PrePlay'
    }
  };
}

function marketTypeIdForSeed(seed: MarketSeed): string {
  if (seed.marketName.includes('Total')) {
    return seed.subCategoryId === 'MLB' ? 'BASEBALL_OVER_UNDER_TOTAL_RUNS' : 'FOOTBALL_OVER_UNDER_TOTAL_GOALS';
  }
  if (seed.marketName.includes('Spread') || seed.marketName.includes('Run Line')) {
    return seed.subCategoryId === 'MLB' ? 'BASEBALL_HANDICAP' : 'FOOTBALL_FULL_TIME_RESULT_HANDICAP';
  }
  if (seed.marketName === 'Moneyline') {
    if (seed.subCategoryId === 'ICEHKY') {
      return 'ICEHKY_MONEYLINE';
    }
    return seed.subCategoryId === 'MLB' ? 'BASEBALL_MONEYLINE' : 'BBALL_MONEYLINE';
  }
  if (seed.marketName === 'Both Teams To Score') {
    return 'FOOTBALL_BOTH_TEAMS_TO_SCORE';
  }
  return seed.subCategoryId === 'TENNIS' ? 'TENNIS_WINNER' : 'FOOTBALL_FULL_TIME_RESULT';
}

function toMarketOutcomes(outcomes: PricePoint[]): MarketOutcome[] {
  return outcomes.map((outcome) => ({
    outcomeId: outcome.outcomeId,
    outcomeName: outcome.outcomeName
  }));
}

function createOutcomes(seed: MarketSeed, index: number): PricePoint[] {
  const validAt = new Date(now - (index + 1) * 12_000).toISOString();
  const [home, away = 'Field'] = seed.eventName.split(' v ');
  const base = 1.55 + (index % 8) * 0.12;

  if (seed.marketName.includes('Total')) {
    return [
      price('over', 'Over', base + 0.22, index, validAt),
      price('under', 'Under', 2.06 - (index % 5) * 0.05, index + 1, validAt)
    ];
  }

  if (seed.marketName.includes('Spread') || seed.marketName.includes('Run Line')) {
    return [
      price('home-spread', `${home} -${(index % 6) + 1.5}`, 1.9 + (index % 3) * 0.03, index, validAt),
      price('away-spread', `${away} +${(index % 6) + 1.5}`, 1.92 - (index % 3) * 0.02, index + 1, validAt)
    ];
  }

  if (seed.marketName === 'Tournament Winner') {
    return [
      price('favorite', 'Favorite', 4.2 + (index % 4), index, validAt),
      price('contender', 'Contender', 7.5 + (index % 5), index + 1, validAt),
      price('field', 'Field', 2.3 + (index % 3) * 0.4, index + 2, validAt)
    ];
  }

  if (seed.marketName === 'Set Betting') {
    return [
      price('home-2-0', `${home} 2-0`, 2.4, index, validAt),
      price('home-2-1', `${home} 2-1`, 4.1, index + 1, validAt),
      price('away-2-1', `${away} 2-1`, 4.8, index + 2, validAt)
    ];
  }

  const outcomes = [
    price(slug(home), home, base + 0.35, index, validAt),
    price(slug(away), away, 2.15 + (index % 6) * 0.18, index + 1, validAt)
  ];

  if (seed.marketName === 'Match Winner') {
    outcomes.splice(1, 0, price('draw', 'Draw', 3.25 + (index % 4) * 0.15, index + 2, validAt));
  }

  return outcomes;
}

function price(outcomeId: string, outcomeName: string, value: number, index: number, validAt: string): PricePoint {
  return {
    outcomeId,
    outcomeName,
    side: 'For',
    price: Number(value.toFixed(2)),
    liquidity: 1800 + index * 540 + (index % 3) * 700,
    change: Number((((index % 5) - 2) * 0.03).toFixed(2)),
    validAt
  };
}

function withLayPrice(back: PricePoint): PricePoint[] {
  return [
    back,
    {
      ...back,
      side: 'Against',
      price: Number((back.price + 0.04).toFixed(2)),
      liquidity: Math.max(100, Math.round(back.liquidity * 0.82))
    }
  ];
}

function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
