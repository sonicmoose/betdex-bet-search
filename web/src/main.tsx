import React from 'react';
import ReactDOM from 'react-dom/client';
import { Activity, BarChart3, Bell, Clock3, Database, Search, SlidersHorizontal } from 'lucide-react';
import { getMarketUpdates, markets, searchMarkets } from './mockApi';
import type { Market, MarketSearchInput, MarketUpdate } from './types';
import './styles.css';

const numberFormatter = new Intl.NumberFormat('en-US', {
  maximumFractionDigits: 2
});

const dateFormatter = new Intl.DateTimeFormat('en-US', {
  month: 'short',
  day: 'numeric',
  hour: '2-digit',
  minute: '2-digit'
});

const categories = Array.from(new Map(markets.map((market) => [market.categoryId, market.categoryName])).entries());

function App() {
  const [filters, setFilters] = React.useState<MarketSearchInput>({
    text: '',
    status: 'Any',
    inPlay: 'Any',
    categoryId: 'Any'
  });
  const [results, setResults] = React.useState<Market[]>(markets);
  const [selectedMarketId, setSelectedMarketId] = React.useState(markets[0]?.marketId ?? '');
  const [updates, setUpdates] = React.useState<MarketUpdate[]>([]);
  const [loading, setLoading] = React.useState(false);

  const selectedMarket = results.find((market) => market.marketId === selectedMarketId)
    ?? markets.find((market) => market.marketId === selectedMarketId)
    ?? results[0]
    ?? markets[0];

  React.useEffect(() => {
    let active = true;
    setLoading(true);
    searchMarkets(filters).then((items) => {
      if (!active) {
        return;
      }
      setResults(items);
      if (items.length > 0 && !items.some((item) => item.marketId === selectedMarketId)) {
        setSelectedMarketId(items[0].marketId);
      }
      setLoading(false);
    });
    return () => {
      active = false;
    };
  }, [filters, selectedMarketId]);

  React.useEffect(() => {
    if (!selectedMarket) {
      setUpdates([]);
      return;
    }
    let active = true;
    getMarketUpdates(selectedMarket.marketId).then((items) => {
      if (active) {
        setUpdates(items);
      }
    });
    return () => {
      active = false;
    };
  }, [selectedMarket]);

  return (
    <main className="app-shell">
      <header className="topbar">
        <div>
          <p className="eyebrow">BetDEX Indexer</p>
          <h1>Market Search</h1>
        </div>
        <div className="status-strip" aria-label="Service status">
          <span><Database size={16} /> Mock API</span>
          <span><Bell size={16} /> Subscriptions ready</span>
        </div>
      </header>

      <section className="toolbar" aria-label="Market filters">
        <label className="search-box">
          <Search size={18} />
          <input
            value={filters.text}
            onChange={(event) => setFilters({ ...filters, text: event.target.value })}
            placeholder="Search market, event, sport, league"
          />
        </label>
        <Select
          label="Status"
          value={filters.status}
          values={['Any', 'Open', 'Suspended', 'Settled', 'Closed']}
          onChange={(status) => setFilters({ ...filters, status: status as MarketSearchInput['status'] })}
        />
        <Select
          label="In-play"
          value={filters.inPlay}
          values={['Any', 'Yes', 'No']}
          onChange={(inPlay) => setFilters({ ...filters, inPlay: inPlay as MarketSearchInput['inPlay'] })}
        />
        <label className="select-wrap">
          <span>Category</span>
          <select value={filters.categoryId} onChange={(event) => setFilters({ ...filters, categoryId: event.target.value })}>
            <option value="Any">Any</option>
            {categories.map(([id, name]) => (
              <option key={id} value={id}>{name}</option>
            ))}
          </select>
        </label>
      </section>

      <section className="workspace">
        <aside className="results-panel" aria-label="Market results">
          <div className="panel-heading">
            <div>
              <p className="eyebrow">Results</p>
              <h2>{loading ? 'Searching' : `${results.length} markets`}</h2>
            </div>
            <SlidersHorizontal size={18} />
          </div>
          <div className="result-list">
            {results.map((market) => (
              <button
                key={market.marketId}
                className={`market-row ${market.marketId === selectedMarket?.marketId ? 'selected' : ''}`}
                onClick={() => setSelectedMarketId(market.marketId)}
              >
                <span className="market-row-title">{market.eventName}</span>
                <span className="market-row-subtitle">{market.name} · {market.subCategoryName}</span>
                <span className="market-row-meta">
                  <Status status={market.status} />
                  {market.inPlay && <span className="live-dot">Live</span>}
                </span>
              </button>
            ))}
            {results.length === 0 && <p className="empty-state">No markets match the current filters.</p>}
          </div>
        </aside>

        {selectedMarket && (
          <section className="detail-panel" aria-label="Selected market">
            <div className="market-header">
              <div>
                <p className="eyebrow">{selectedMarket.categoryName} · {selectedMarket.subCategoryName}</p>
                <h2>{selectedMarket.eventName}</h2>
                <p>{selectedMarket.name}</p>
              </div>
              <Status status={selectedMarket.status} />
            </div>

            <div className="metric-grid">
              <Metric icon={<Clock3 size={18} />} label="Starts" value={dateFormatter.format(new Date(selectedMarket.startsAt))} />
              <Metric icon={<Activity size={18} />} label="Matched" value={`$${numberFormatter.format(selectedMarket.matched)}`} />
              <Metric icon={<BarChart3 size={18} />} label="Liquidity" value={`$${numberFormatter.format(selectedMarket.liquidity)}`} />
            </div>

            <div className="two-column">
              <section className="surface">
                <div className="section-title">
                  <h3>Prices</h3>
                  <span>{selectedMarket.outcomes.length} outcomes</span>
                </div>
                <table className="price-table">
                  <thead>
                    <tr>
                      <th>Outcome</th>
                      <th>Side</th>
                      <th>Price</th>
                      <th>Liquidity</th>
                      <th>Change</th>
                    </tr>
                  </thead>
                  <tbody>
                    {selectedMarket.outcomes.map((price) => (
                      <tr key={price.outcomeId}>
                        <td>{price.outcomeName}</td>
                        <td>{price.side}</td>
                        <td>{price.price.toFixed(2)}</td>
                        <td>${numberFormatter.format(price.liquidity)}</td>
                        <td className={price.change >= 0 ? 'positive' : 'negative'}>{price.change >= 0 ? '+' : ''}{price.change.toFixed(2)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </section>

              <section className="surface">
                <div className="section-title">
                  <h3>Update History</h3>
                  <span>{updates.length} latest</span>
                </div>
                <div className="timeline">
                  {updates.map((update) => (
                    <article key={`${update.marketId}-${update.messageType}-${update.receivedAt}`} className="timeline-item">
                      <span className="timeline-dot" />
                      <div>
                        <strong>{update.messageType}</strong>
                        <time>{dateFormatter.format(new Date(update.receivedAt))}</time>
                      </div>
                    </article>
                  ))}
                </div>
              </section>
            </div>

            <section className="surface raw-panel">
              <div className="section-title">
                <h3>Raw Source</h3>
                <span>{selectedMarket.marketId}</span>
              </div>
              <pre>{JSON.stringify(selectedMarket.raw, null, 2)}</pre>
            </section>
          </section>
        )}
      </section>
    </main>
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

function Metric({ icon, label, value }: { icon: React.ReactNode; label: string; value: string }) {
  return (
    <div className="metric">
      {icon}
      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
