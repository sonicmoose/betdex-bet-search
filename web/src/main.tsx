import React from 'react';
import ReactDOM from 'react-dom/client';
import { Search } from 'lucide-react';
import { dataSourceLabel, initialMarkets, searchMarkets } from './api';
import type { InPlayFilter, Market, MarketSearchInput, MarketSearchResult, MarketSort, MarketStatus, PricePoint } from './types';
import './styles.css';

const numberFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 0
});

const dateFormatter = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit'
});

const betdexBaseUrl = 'https://betdex.com';
const pageSizes = [10, 20, 50];
const defaultSportOptions = ['FOOTBALL', 'CRICKET', 'ICEHKY', 'MLB'].map((value) => ({ value, label: value }));
const defaultLeagueOptions = ['ALGE', 'EFL', 'EPL', 'FIFAWC', 'LALIGA', 'LALIGA2', 'MLB', 'NHL'].map((value) => ({ value, label: value }));
const sportIds = new Set(defaultSportOptions.map((option) => option.value));
const nonLeagueIds = new Set(['SPORTS', 'FOOTBALL', 'CRICKET', 'ICEHKY']);

function App() {
  const [filters, setFilters] = React.useState<MarketSearchInput>({
    text: '',
    statuses: [],
    inPlay: [],
    subCategoryIds: [],
    eventGroupIds: [],
    sort: 'Relevance',
    page: 1,
    pageSize: 10
  });
  const [searchResult, setSearchResult] = React.useState<MarketSearchResult>({
    items: initialMarkets,
    total: initialMarkets.length,
    page: 1,
    pageSize: 10
  });
  const [filterOptions, setFilterOptions] = React.useState(() => deriveFilterOptions(initialMarkets));
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  const totalPages = Math.max(1, Math.ceil(searchResult.total / searchResult.pageSize));
  const firstResult = searchResult.total === 0 ? 0 : (searchResult.page - 1) * searchResult.pageSize + 1;
  const lastResult = Math.min(searchResult.total, searchResult.page * searchResult.pageSize);

  React.useEffect(() => {
    let active = true;
    setLoading(true);
    setError(null);
    searchMarkets(filters).then((result) => {
      if (active) {
        setSearchResult(result);
        setFilterOptions((current) => mergeFilterOptions(current, deriveFilterOptions(result.items)));
        setLoading(false);
      }
    }).catch((reason: unknown) => {
      if (active) {
        setError(reason instanceof Error ? reason.message : 'Search failed');
        setLoading(false);
      }
    });
    return () => {
      active = false;
    };
  }, [filters]);

  function updateFilters(next: Partial<MarketSearchInput>) {
    setFilters((current) => ({ ...current, ...next, page: 1 }));
  }

  function setPage(page: number) {
    setFilters((current) => ({ ...current, page }));
  }

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">BetDEX discovery</p>
          <h1>Market Search</h1>
        </div>
        <div className="topbar-meta">
          <span>{loading ? 'Searching' : `${searchResult.total} markets`}</span>
          <span>{dataSourceLabel}</span>
        </div>
      </header>

      <section className="search-command" aria-label="Search and ordering">
        <label className="search-box">
          <Search size={20} />
          <input
            value={filters.text}
            onChange={(event) => updateFilters({ text: event.target.value })}
            placeholder="Search team, player, league, sport, or market"
          />
        </label>
        <Select
          label="Order by"
          value={filters.sort}
          values={['Relevance', 'Start time', 'Matched', 'Liquidity']}
          onChange={(sort) => updateFilters({ sort: sort as MarketSort })}
        />
      </section>

      <section className="search-layout">
        <aside className="filter-panel" aria-label="Market filters">
          <FilterGroup
            label="Sport"
            values={filterOptions.sports}
            selected={filters.subCategoryIds}
            onChange={(subCategoryIds) => updateFilters({ subCategoryIds })}
          />
          <FilterGroup
            label="League"
            values={filterOptions.leagues}
            selected={filters.eventGroupIds}
            onChange={(eventGroupIds) => updateFilters({ eventGroupIds })}
          />
          <FilterGroup
            label="Status"
            values={['Open', 'Suspended', 'Settled', 'Closed'].map((status) => ({ value: status, label: status }))}
            selected={filters.statuses}
            onChange={(statuses) => updateFilters({ statuses: statuses as MarketStatus[] })}
          />
          <FilterGroup
            label="In-play"
            values={[
              { value: 'Yes', label: 'Live' },
              { value: 'No', label: 'Pre-match' }
            ]}
            selected={filters.inPlay}
            onChange={(inPlay) => updateFilters({ inPlay: inPlay as InPlayFilter[] })}
          />
        </aside>

        <section className="market-results" aria-label="Market results">
          <div className="results-head">
            <span>Event / Market</span>
            <span>Matched</span>
            <span>Prices</span>
          </div>

          <div className="market-list">
            {error && <p className="empty-state">Search failed: {error}</p>}
            {searchResult.items.map((market) => (
              <MarketRow key={market.marketId} market={market} />
            ))}
            {searchResult.items.length === 0 && <p className="empty-state">No markets match the current filters.</p>}
          </div>

          <div className="pagination" aria-label="Pagination">
            <span>{firstResult}-{lastResult} of {searchResult.total}</span>
            <div className="page-controls">
              <button disabled={searchResult.page <= 1} onClick={() => setPage(searchResult.page - 1)}>Previous</button>
              <span>Page {searchResult.page} of {totalPages}</span>
              <button disabled={searchResult.page >= totalPages} onClick={() => setPage(searchResult.page + 1)}>Next</button>
            </div>
            <label>
              <span>Rows</span>
              <select
                value={filters.pageSize}
                onChange={(event) => updateFilters({ pageSize: Number(event.target.value) })}
              >
                {pageSizes.map((size) => (
                  <option key={size} value={size}>{size}</option>
                ))}
              </select>
            </label>
          </div>
        </section>
      </section>
    </main>
  );
}

function MarketRow({ market }: { market: Market }) {
  const startsAt = new Date(market.startsAt);

  return (
    <article className="market-row">
      <div className="market-info">
        <div className="market-title-line">
          <h2>{market.eventName}</h2>
          {market.inPlay && <span className="live-pill">Live</span>}
          <Status status={market.status} />
        </div>
        <p>{market.name} · Sport {market.subCategoryId || 'Unknown'} · League {market.eventGroupId || 'Unknown'}</p>
        <div className="market-meta">
          <span>{Number.isNaN(startsAt.getTime()) ? 'Time TBC' : dateFormatter.format(startsAt)}</span>
          <span>Liquidity ${numberFormatter.format(market.liquidity)}</span>
        </div>
      </div>

      <div className="matched-cell">
        <strong>${numberFormatter.format(market.matched)}</strong>
      </div>

      <div className="price-strip" aria-label={`${market.eventName} prices`}>
        {market.outcomes.map((price) => (
          <PriceLink key={price.outcomeId} market={market} price={price} />
        ))}
        {market.outcomes.length === 0 && <MarketLink market={market} />}
      </div>
    </article>
  );
}

function PriceLink({ market, price }: { market: Market; price: PricePoint }) {
  const href = `${betdexBaseUrl}/markets/${encodeURIComponent(market.marketId)}?outcome=${encodeURIComponent(price.outcomeId)}`;

  return (
    <a className="price-button" href={href} target="_blank" rel="noreferrer">
      <span>{price.outcomeName}</span>
      <strong>{price.price.toFixed(2)}</strong>
      <small>${numberFormatter.format(price.liquidity)}</small>
    </a>
  );
}

function MarketLink({ market }: { market: Market }) {
  const href = `${betdexBaseUrl}/markets/${encodeURIComponent(market.marketId)}`;

  return (
    <a className="price-button" href={href} target="_blank" rel="noreferrer">
      <span>Market</span>
      <strong>Open</strong>
      <small>BetDEX</small>
    </a>
  );
}

function FilterGroup({
  label,
  values,
  selected,
  onChange
}: {
  label: string;
  values: Array<{ value: string; label: string }>;
  selected: string[];
  onChange: (selected: string[]) => void;
}) {
  const [expanded, setExpanded] = React.useState(false);
  const hasOverflow = values.length > 5;
  const visibleValues = expanded ? values : values.slice(0, 5);

  function toggle(value: string) {
    onChange(selected.includes(value)
      ? selected.filter((item) => item !== value)
      : [...selected, value]);
  }

  return (
    <fieldset className="filter-group">
      <legend>{label}</legend>
      <div className="filter-options">
        {visibleValues.map((item) => {
          const checked = selected.includes(item.value);
          return (
            <label key={item.value} className={`check-row ${checked ? 'selected' : ''}`}>
              <input
                type="checkbox"
                checked={checked}
                onChange={() => toggle(item.value)}
              />
              <span>{item.label}</span>
            </label>
          );
        })}
      </div>
      {hasOverflow && (
        <button type="button" className="more-button" onClick={() => setExpanded(!expanded)}>
          {expanded ? 'Less' : `More (${values.length - 5})`}
        </button>
      )}
    </fieldset>
  );
}

function Select({ label, value, values, onChange }: { label: string; value: string; values: string[]; onChange: (value: string) => void }) {
  return (
    <label className="select-wrap">
      <span>{label}</span>
      <select value={value} onChange={(event) => onChange(event.target.value)}>
        {values.map((item) => (
          <option key={item} value={item}>{item}</option>
        ))}
      </select>
    </label>
  );
}

function Status({ status }: { status: string }) {
  return <span className={`status status-${status.toLowerCase()}`}>{status}</span>;
}

function deriveFilterOptions(items: Market[]) {
  return {
    sports: uniqueOptions([
      ...defaultSportOptions,
      ...items
        .filter((market) => sportIds.has(market.subCategoryId))
        .map((market) => ({ value: market.subCategoryId, label: market.subCategoryId }))
    ]),
    leagues: uniqueOptions([
      ...defaultLeagueOptions,
      ...items
        .filter((market) => isBetdexId(market.eventGroupId) && !nonLeagueIds.has(market.eventGroupId))
        .map((market) => ({ value: market.eventGroupId, label: market.eventGroupId }))
    ])
  };
}

function mergeFilterOptions(left: FilterOptions, right: FilterOptions): FilterOptions {
  return {
    sports: uniqueOptions([...left.sports, ...right.sports]),
    leagues: uniqueOptions([...left.leagues, ...right.leagues])
  };
}

function uniqueOptions(options: Array<{ value: string; label: string }>) {
  return Array.from(new Map(options
    .filter((option) => option.value && option.label)
    .map((option) => [option.value, option])).values())
    .sort((left, right) => left.label.localeCompare(right.label));
}

function isBetdexId(value: string) {
  return /^[A-Z0-9]+$/.test(value);
}

interface FilterOptions {
  sports: Array<{ value: string; label: string }>;
  leagues: Array<{ value: string; label: string }>;
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
