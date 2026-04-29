import React from 'react';
import ReactDOM from 'react-dom/client';
import { Search } from 'lucide-react';
import { markets, searchMarkets } from './mockApi';
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

const categories = Array.from(new Map(markets.map((market) => [market.categoryId, market.categoryName])).entries());
const subCategories = Array.from(new Map(markets.map((market) => [market.subCategoryId, market.subCategoryName])).entries())
  .sort((left, right) => left[1].localeCompare(right[1]));
const betdexBaseUrl = 'https://betdex.com';
const pageSizes = [10, 20, 50];

function App() {
  const [filters, setFilters] = React.useState<MarketSearchInput>({
    text: '',
    statuses: [],
    inPlay: [],
    categoryIds: [],
    subCategoryIds: [],
    sort: 'Relevance',
    page: 1,
    pageSize: 10
  });
  const [searchResult, setSearchResult] = React.useState<MarketSearchResult>({
    items: markets.slice(0, 10),
    total: markets.length,
    page: 1,
    pageSize: 10
  });
  const [loading, setLoading] = React.useState(false);

  const totalPages = Math.max(1, Math.ceil(searchResult.total / searchResult.pageSize));
  const firstResult = searchResult.total === 0 ? 0 : (searchResult.page - 1) * searchResult.pageSize + 1;
  const lastResult = Math.min(searchResult.total, searchResult.page * searchResult.pageSize);

  React.useEffect(() => {
    let active = true;
    setLoading(true);
    searchMarkets(filters).then((result) => {
      if (active) {
        setSearchResult(result);
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
          <span>Mock data</span>
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
            values={categories.map(([id, name]) => ({ value: id, label: name }))}
            selected={filters.categoryIds}
            onChange={(categoryIds) => updateFilters({ categoryIds })}
          />
          <FilterGroup
            label="League"
            values={subCategories.map(([id, name]) => ({ value: id, label: name }))}
            selected={filters.subCategoryIds}
            onChange={(subCategoryIds) => updateFilters({ subCategoryIds })}
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
  return (
    <article className="market-row">
      <div className="market-info">
        <div className="market-title-line">
          <h2>{market.eventName}</h2>
          {market.inPlay && <span className="live-pill">Live</span>}
          <Status status={market.status} />
        </div>
        <p>{market.name} · {market.categoryName} · {market.subCategoryName}</p>
        <div className="market-meta">
          <span>{dateFormatter.format(new Date(market.startsAt))}</span>
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

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
