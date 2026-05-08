export const sportGroups = [
  { value: 'BASEBALL', label: 'Baseball', subCategoryIds: ['BASEBALL', 'MLB'] },
  { value: 'BBALL', label: 'Basketball', subCategoryIds: ['BBALL'] },
  { value: 'FOOTBALL', label: 'Football', subCategoryIds: ['FOOTBALL'] },
  { value: 'ICEHKY', label: 'Ice Hockey', subCategoryIds: ['ICEHKY'] },
  { value: 'TENNIS', label: 'Tennis', subCategoryIds: ['TENNIS'] }
];

export const sportLabels: Record<string, string> = Object.fromEntries(
  sportGroups.flatMap((group) => group.subCategoryIds.map((id) => [id, group.label]))
);

export const leagueLabels: Record<string, string> = {
  AUSL: 'A-League',
  ALGE: 'Algeria Ligue 1',
  ARLP: 'Argentina Liga Profesional',
  CHALWXI: 'ATP Challenger Wuxi (China Men Singles)',
  AROME: 'ATP Rome',
  AALW: 'Australia A-League (Women)',
  AUSBUND: 'Austrian Bundesliga',
  BEVL: 'Belarus Vysshaya Liga',
  BFD: 'Belgian Jupiler Pro',
  BLVDP: 'Bolivia Division Professional',
  BRAZILA: 'Brasileiro Serie A',
  BRAZILB: 'Brasileiro Serie B',
  BRAZILC: 'Brasileiro Serie C',
  BBW: 'Brazil Brasileiro Women',
  BUPL: 'Bulgaria Parva Liga',
  CCDL: 'Chile Copa de la Liga',
  CLDA: 'Chile Liga de Ascenso',
  CHSLE: 'China Super League',
  CLPA: 'Colombia Primera A',
  CLPB: 'Colombia Primera B',
  COPAITL: 'Coppa Italia',
  CROD1: 'Croatia Division 1',
  CPRNL: 'Croatia Prva NL',
  SUPLIGA: 'Danish Superliga',
  DEND1: 'Denmark Division 1',
  ERD: 'Dutch Eredivisie',
  ECLIP: 'Ecuador Liga Pro',
  EGYP: 'Egyptian Premier League',
  ENL: 'England National League',
  EFL: 'English Championship',
  EFL1: 'English Football League 1',
  EFL2: 'English Football League 2',
  ENNROTH: 'English National League North',
  ENNSO: 'English National League South',
  EPL: 'English Premier League',
  EPL2: 'English Premier League 2',
  EWSL: "English Women's Super League",
  EULEAGUE: 'EuroLeague',
  FIN: 'Finnish Veikkausliiga',
  LIGUE1: 'French Ligue 1',
  LIGUE2: 'French Ligue 2',
  '3LIG': 'German 3 Liga',
  BUND: 'German Bundesliga',
  BUND2: 'German Bundesliga 2',
  GPL: 'Ghanaian Premier League',
  GSL: 'Greece Super League',
  HKPL: 'Hong Kong Premier League',
  ISL: 'Indian Super League',
  INDOSL: 'Indonesia Super League',
  IRD1: 'Ireland Division 1',
  ILPD: 'Ireland Premier Division',
  SERIEA: 'Italian Serie A',
  SERIEB: 'Italian Serie B',
  JLEAGUE: 'J-League',
  JAMP: 'Jamaica Premier League',
  KPL: 'Kenyan Premier League',
  LP2: 'Liga Portugal 2',
  MLB: 'Major League Baseball',
  MLSNP: 'MLS Next Pro',
  NBA: 'NBA',
  NHL: 'NHL',
  N1DV: 'Norway 1st Division',
  NORWAY: 'Norway Eliteserien',
  PED1: 'Peru Liga 1',
  PDIV1: 'Poland Division 1',
  POEKS: 'Poland Ekstraklasa',
  PRIMLIGA: 'Portugal Primeira Liga',
  QSL: 'Qatar Stars League',
  RSL: 'Romania Superliga',
  RUSPL: 'Russian Premier League',
  SAUPL: 'Saudi Professional League',
  SCOT: 'Scotland Championship',
  SCL1: 'Scottish League 1',
  SCOT2: 'Scottish League 2',
  SPL: 'Scottish Premiership',
  SLNL: 'Slovakia Nike Liga',
  SKG1: 'South Korea K League 1',
  LALIGA2: 'Spanish La Liga 2',
  LALIGA: 'Spanish LaLiga',
  SWEDE: 'Swedish Allsvenskan',
  SSLG: 'Swiss Super League',
  SWCL: 'Switzerland Challenge League',
  TUR1LIG: 'Turkey 1. Lig',
  SULG: 'Turkey Super Lig',
  URUPD: 'Uruguay Primera Division',
  URUSEG: 'Uruguay Segunda Division',
  MLS: 'US MLS',
  USLC: 'USL Championship',
  VENE: 'Venezuela Liga FUTVE',
  WNBA: 'WNBA',
  WROME: 'WTA Rome'
};

export const marketTypeGroups = [
  {
    value: 'RESULT_WINNER',
    label: 'Result / Winner',
    marketTypeIds: [
      'FOOTBALL_FULL_TIME_RESULT',
      'FOOTBALL_HALF_TIME_RESULT',
      'TENNIS_WINNER',
      'TENNIS_SET_X_WINNER'
    ]
  },
  {
    value: 'MONEYLINE',
    label: 'Moneyline',
    marketTypeIds: [
      'BBALL_HALF_TIME_MONEYLINE',
      'BBALL_MONEYLINE',
      'BASEBALL_HALF_TIME_MONEYLINE',
      'BASEBALL_MONEYLINE',
      'ICEHKY_FIRST_PERIOD_MONEYLINE',
      'ICEHKY_SECOND_PERIOD_MONEYLINE',
      'ICEHKY_THIRD_PERIOD_MONEYLINE',
      'ICEHKY_MONEYLINE'
    ]
  },
  {
    value: 'HANDICAP',
    label: 'Handicap',
    marketTypeIds: [
      'FOOTBALL_FULL_TIME_RESULT_HANDICAP',
      'FOOTBALL_HALF_TIME_RESULT_HANDICAP',
      'BASEBALL_HANDICAP',
      'BASEBALL_HALF_TIME_HANDICAP',
      'ICEHKY_FIRST_PERIOD_HANDICAP',
      'ICEHKY_SECOND_PERIOD_HANDICAP',
      'ICEHKY_THIRD_PERIOD_HANDICAP',
      'ICEHKY_HANDICAP'
    ]
  },
  {
    value: 'TOTALS',
    label: 'Totals',
    marketTypeIds: [
      'FOOTBALL_OVER_UNDER_TOTAL_GOALS',
      'FOOTBALL_OVER_UNDER_HALF_TIME_TOTAL_GOALS',
      'BASEBALL_OVER_UNDER_TOTAL_RUNS',
      'BASEBALL_OVER_UNDER_HALF_TIME_TOTAL_RUNS',
      'ICEHKY_OVER_UNDER_TOTAL_GOALS',
      'ICEHKY_OVER_UNDER_FIRST_PERIOD_TOTAL_GOALS',
      'ICEHKY_OVER_UNDER_SECOND_PERIOD_TOTAL_GOALS',
      'ICEHKY_OVER_UNDER_THIRD_PERIOD_TOTAL_GOALS'
    ]
  },
  {
    value: 'BOTH_TEAMS_TO_SCORE',
    label: 'Both Teams To Score',
    marketTypeIds: ['FOOTBALL_BOTH_TEAMS_TO_SCORE']
  },
  {
    value: 'CORRECT_SCORE',
    label: 'Correct Score',
    marketTypeIds: ['FOOTBALL_FULL_TIME_CORRECT_SCORE']
  }
];

export function expandMarketTypeGroups(values: string[]): string[] {
  const byGroup = new Map(marketTypeGroups.map((group) => [group.value, group.marketTypeIds]));
  return Array.from(new Set(values.flatMap((value) => byGroup.get(value) ?? [value])));
}

export function expandSportGroups(values: string[]): string[] {
  const byGroup = new Map(sportGroups.map((group) => [group.value, group.subCategoryIds]));
  return Array.from(new Set(values.flatMap((value) => byGroup.get(value) ?? [value])));
}

export function sportGroupFor(subCategoryId: string): string | undefined {
  return sportGroups.find((group) => group.subCategoryIds.includes(subCategoryId))?.value;
}

export function marketTypeGroupFor(marketTypeId: string): string | undefined {
  return marketTypeGroups.find((group) => group.marketTypeIds.includes(marketTypeId))?.value;
}

export function marketTypeLabel(value: string): string {
  return marketTypeGroups.find((group) => group.value === value)?.label ?? humanizeId(value);
}

export function leagueLabel(value: string): string {
  return leagueLabels[value] ?? humanizeId(value);
}

export function sportLabel(value: string): string {
  return sportLabels[value] ?? humanizeId(value);
}

function humanizeId(value: string): string {
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.length <= 4 ? part : part.charAt(0) + part.slice(1).toLowerCase())
    .join(' ');
}
