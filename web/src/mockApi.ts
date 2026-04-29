import type { Market, MarketSearchInput, MarketSearchResult, MarketStatus, PricePoint } from './types';

const now = Date.now();

interface MarketSeed {
  categoryId: string;
  categoryName: string;
  subCategoryId?: string;
  subCategoryName: string;
  eventName: string;
  marketName: string;
  inPlay?: boolean;
  status?: MarketStatus;
  startsInHours: number;
}

const seeds: MarketSeed[] = [
  { categoryId: 'football', categoryName: 'Football', subCategoryName: 'Premier League', eventName: 'Arsenal v Chelsea', marketName: 'Match Winner', inPlay: true, startsInHours: -0.5 },
  { categoryId: 'football', categoryName: 'Football', subCategoryName: 'Premier League', eventName: 'Liverpool v Manchester City', marketName: 'Total Goals', startsInHours: 20 },
  { categoryId: 'football', categoryName: 'Football', subCategoryName: 'La Liga', eventName: 'Barcelona v Real Madrid', marketName: 'Match Winner', startsInHours: 28 },
  { categoryId: 'football', categoryName: 'Football', subCategoryName: 'Champions League', eventName: 'Bayern Munich v Inter Milan', marketName: 'Both Teams To Score', startsInHours: 52 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'San Francisco Giants v Los Angeles Dodgers', marketName: 'Total Runs', startsInHours: 24 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'New York Yankees v Boston Red Sox', marketName: 'Moneyline', startsInHours: 6 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'Chicago Cubs v St Louis Cardinals', marketName: 'Run Line', startsInHours: 30 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'Atlanta Braves v New York Mets', marketName: 'Total Runs', inPlay: true, startsInHours: -1 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'Houston Astros v Seattle Mariners', marketName: 'Moneyline', startsInHours: 12 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'Texas Rangers v Oakland Athletics', marketName: 'First Five Innings', startsInHours: 18 },
  { categoryId: 'american-football', categoryName: 'American Football', subCategoryName: 'NFL', eventName: 'Kansas City Chiefs v Las Vegas Raiders', marketName: 'Moneyline', startsInHours: 72 },
  { categoryId: 'american-football', categoryName: 'American Football', subCategoryName: 'NFL', eventName: 'Dallas Cowboys v Philadelphia Eagles', marketName: 'Point Spread', startsInHours: 80 },
  { categoryId: 'american-football', categoryName: 'American Football', subCategoryName: 'NFL', eventName: 'Buffalo Bills v Miami Dolphins', marketName: 'Total Points', startsInHours: 88 },
  { categoryId: 'american-football', categoryName: 'American Football', subCategoryName: 'NFL', eventName: 'Green Bay Packers v Chicago Bears', marketName: 'Moneyline', startsInHours: 96 },
  { categoryId: 'american-football', categoryName: 'American Football', subCategoryName: 'NFL', eventName: 'San Francisco 49ers v Seattle Seahawks', marketName: 'Point Spread', status: 'Suspended', startsInHours: 104 },
  { categoryId: 'basketball', categoryName: 'Basketball', subCategoryName: 'NBA', eventName: 'Boston Celtics v New York Knicks', marketName: 'Point Spread', startsInHours: 5 },
  { categoryId: 'basketball', categoryName: 'Basketball', subCategoryName: 'NBA', eventName: 'Los Angeles Lakers v Phoenix Suns', marketName: 'Moneyline', startsInHours: 9 },
  { categoryId: 'basketball', categoryName: 'Basketball', subCategoryName: 'NBA', eventName: 'Denver Nuggets v Minnesota Timberwolves', marketName: 'Total Points', inPlay: true, startsInHours: -0.75 },
  { categoryId: 'basketball', categoryName: 'Basketball', subCategoryName: 'EuroLeague', eventName: 'Real Madrid v Olympiacos', marketName: 'Moneyline', startsInHours: 34 },
  { categoryId: 'tennis', categoryName: 'Tennis', subCategoryName: 'ATP', eventName: 'Miami Open Final', marketName: 'Match Winner', status: 'Suspended', inPlay: true, startsInHours: -2 },
  { categoryId: 'tennis', categoryName: 'Tennis', subCategoryName: 'WTA', eventName: 'Swiatek v Gauff', marketName: 'Match Winner', startsInHours: 14 },
  { categoryId: 'tennis', categoryName: 'Tennis', subCategoryName: 'ATP', eventName: 'Alcaraz v Sinner', marketName: 'Set Betting', startsInHours: 40 },
  { categoryId: 'ice-hockey', categoryName: 'Ice Hockey', subCategoryName: 'NHL', eventName: 'Toronto Maple Leafs v Boston Bruins', marketName: 'Moneyline', startsInHours: 7 },
  { categoryId: 'ice-hockey', categoryName: 'Ice Hockey', subCategoryName: 'NHL', eventName: 'New York Rangers v New Jersey Devils', marketName: 'Total Goals', startsInHours: 11 },
  { categoryId: 'golf', categoryName: 'Golf', subCategoryName: 'PGA Tour', eventName: 'US Open', marketName: 'Tournament Winner', startsInHours: 120 },
  { categoryId: 'cricket', categoryName: 'Cricket', subCategoryName: 'IPL', eventName: 'Mumbai Indians v Chennai Super Kings', marketName: 'Match Winner', startsInHours: 16 },
  { categoryId: 'mma', categoryName: 'MMA', subCategoryName: 'UFC', eventName: 'Pereira v Ankalaev', marketName: 'Fight Winner', startsInHours: 58 },
  { categoryId: 'rugby', categoryName: 'Rugby', subCategoryName: 'Six Nations', eventName: 'Ireland v France', marketName: 'Match Winner', startsInHours: 64 },
  { categoryId: 'soccer-us', categoryName: 'Soccer', subCategoryName: 'MLS', eventName: 'Inter Miami v Atlanta United', marketName: 'Match Winner', startsInHours: 22 },
  { categoryId: 'esports', categoryName: 'Esports', subCategoryName: 'Counter-Strike', eventName: 'NAVI v Vitality', marketName: 'Match Winner', startsInHours: 3 },
  { categoryId: 'baseball', categoryName: 'Baseball', subCategoryName: 'MLB', eventName: 'Philadelphia Phillies v Washington Nationals', marketName: 'Moneyline', status: 'Closed', startsInHours: -26 },
  { categoryId: 'american-football', categoryName: 'American Football', subCategoryName: 'NFL', eventName: 'Detroit Lions v Minnesota Vikings', marketName: 'Total Points', status: 'Settled', startsInHours: -48 }
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
      market.subCategoryName,
      ...market.outcomes.map((outcome) => outcome.outcomeName)
    ];
    const textMatch = !text || searchable.some((value) => value.toLowerCase().includes(text));
    const statusMatch = input.statuses.length === 0 || input.statuses.includes(market.status);
    const inPlayMatch = input.inPlay.length === 0 || input.inPlay.includes(market.inPlay ? 'Yes' : 'No');
    const categoryMatch = input.categoryIds.length === 0 || input.categoryIds.includes(market.categoryId);
    const leagueMatch = input.subCategoryIds.length === 0 || input.subCategoryIds.includes(market.subCategoryId);
    return textMatch && statusMatch && inPlayMatch && categoryMatch && leagueMatch;
  });

  const sorted = [...filtered].sort((left, right) => {
    switch (input.sort) {
      case 'Start time':
        return new Date(left.startsAt).getTime() - new Date(right.startsAt).getTime();
      case 'Matched':
        return right.matched - left.matched;
      case 'Liquidity':
        return right.liquidity - left.liquidity;
      case 'Relevance':
      default:
        return 0;
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

  return {
    marketId,
    name: seed.marketName,
    eventId: `evt-${slug(seed.eventName)}`,
    eventName: seed.eventName,
    categoryId: seed.categoryId,
    categoryName: seed.categoryName,
    subCategoryId: seed.subCategoryId ?? slug(seed.subCategoryName),
    subCategoryName: seed.subCategoryName,
    status: seed.status ?? 'Open',
    inPlay: seed.inPlay ?? false,
    startsAt: startsAt.toISOString(),
    matched,
    liquidity,
    outcomes: createOutcomes(seed, index),
    raw: {
      marketType: slug(seed.marketName).toUpperCase().replace(/-/g, '_'),
      currencyId: 'USDC',
      inPlayStatus: seed.inPlay ? 'InPlay' : 'PrePlay'
    }
  };
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

function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

function delay(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}
