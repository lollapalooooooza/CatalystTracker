import { useState, useEffect, useRef } from 'react';
import axios from 'axios';

interface Ticker {
  symbol: string;
  name: string;
  sector?: string;
}

interface Props {
  activeTickers: string[];
  selectedSymbol: string;
  onSelect: (symbol: string) => void;
  onAdd: (symbol: string) => void;
  watchlist?: string[];
  onToggleWatchlist?: (symbol: string) => void;
}

const GROUPS: Record<string, string[]> = {
  'Tech': ['AAPL', 'MSFT', 'GOOGL', 'GOOG', 'META', 'AMZN', 'CRM', 'ORCL', 'IBM', 'CSCO', 'NOW', 'WDAY', 'SNOW', 'DELL', 'ADBE'],
  'AI / Chip': ['NVDA', 'AMD', 'TSM', 'AVGO', 'INTC', 'QCOM', 'ARM', 'AMAT', 'LRCX', 'MU', 'MRVL', 'SMCI', 'CRWV', 'TXN', 'ASML'],
  'AI Software': ['AI', 'SOUN', 'SOUNW', 'CRWD', 'ANET', 'IDCC'],
  'EV / Auto': ['TSLA', 'RIVN', 'LCID', 'NIO', 'LI', 'BYDDY', 'F', 'GM', 'STLA', 'TM'],
  'China': ['BABA', 'JD', 'BIDU', 'NIO', 'LI', 'BILI', 'NTES', 'SE', 'MCHI', 'FXI'],
  'Finance': ['V', 'MA', 'GS', 'MS', 'BAC', 'WFC', 'C', 'BLK', 'COIN', 'HOOD', 'MARA'],
  'Media': ['NFLX', 'DIS', 'ROKU', 'WBD', 'ZM'],
  'Consumer': ['COST', 'WMT', 'HD', 'TGT', 'NKE', 'SBUX', 'MCD', 'CMG', 'KO', 'EBAY', 'MELI'],
  'Health': ['UNH', 'JNJ', 'LLY', 'MRNA', 'NVO'],
  'Energy': ['XOM', 'CVX', 'OXY', 'XLE', 'USO'],
  'Telecom': ['T', 'VZ'],
  'Other': ['BA', 'UBER', 'GME', 'AMC', 'MULN', 'SQ', 'FB', 'AMJB', 'GLD', 'XLU', 'XLY', 'DIDI'],
};

export default function StockSelector({ activeTickers, selectedSymbol, onSelect, onAdd, watchlist = [], onToggleWatchlist }: Props) {
  const [query, setQuery] = useState('');
  const [results, setResults] = useState<Ticker[]>([]);
  const [showSearch, setShowSearch] = useState(false);
  const [showPanel, setShowPanel] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);
  const panelRef = useRef<HTMLDivElement>(null);
  const timerRef = useRef<ReturnType<typeof setTimeout>>(undefined);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (searchRef.current && !searchRef.current.contains(e.target as Node)) {
        setShowSearch(false);
      }
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        setShowPanel(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  function handleSearch(q: string) {
    setQuery(q);
    if (timerRef.current) clearTimeout(timerRef.current);
    if (q.length < 1) {
      setResults([]);
      setShowSearch(false);
      return;
    }
    timerRef.current = setTimeout(async () => {
      try {
        const res = await axios.get(`/api/stocks/search?q=${encodeURIComponent(q)}`);
        setResults(res.data);
        setShowSearch(true);
      } catch {
        setResults([]);
      }
    }, 300);
  }

  function handlePick(ticker: Ticker) {
    setQuery('');
    setShowSearch(false);
    setShowPanel(false);
    if (!activeTickers.includes(ticker.symbol)) {
      onAdd(ticker.symbol);
    }
    onSelect(ticker.symbol);
  }

  function handleSelectTicker(sym: string) {
    setShowPanel(false);
    onSelect(sym);
  }

  // Build groups filtered to only tickers that exist in our data
  const activeSet = new Set(activeTickers);
  const renderedGroups = Object.entries(GROUPS)
    .map(([label, symbols]) => ({
      label,
      symbols: symbols.filter((s) => activeSet.has(s)),
    }))
    .filter((g) => g.symbols.length > 0);

  const assigned = new Set(renderedGroups.flatMap((g) => g.symbols));
  const ungrouped = activeTickers.filter((s) => !assigned.has(s)).sort();
  if (ungrouped.length > 0) {
    const otherGroup = renderedGroups.find((g) => g.label === 'Other');
    if (otherGroup) {
      otherGroup.symbols.push(...ungrouped);
    } else {
      renderedGroups.push({ label: 'Other', symbols: ungrouped });
    }
  }

  return (
    <div className="stock-selector">
      {/* Current ticker button — click to open dropdown */}
      <div className="ticker-dropdown-wrapper" ref={panelRef}>
        <button
          className="ticker-current"
          onClick={() => setShowPanel((v) => !v)}
        >
          <span className="ticker-current-symbol">{selectedSymbol || '---'}</span>
          <span className={`ticker-arrow ${showPanel ? 'open' : ''}`}>&#9662;</span>
        </button>

        {showPanel && (
          <div className="ticker-panel">
            {watchlist.length > 0 && (
              <div className="ticker-panel-group" key="Watchlist">
                <div className="ticker-panel-group-label">Watchlist</div>
                <div className="ticker-panel-group-items">
                  {watchlist.filter(s => activeTickers.includes(s)).map((sym) => (
                    <button
                      key={sym}
                      className={`ticker-panel-item watchlist-item ${sym === selectedSymbol ? 'active' : ''}`}
                      onClick={() => handleSelectTicker(sym)}
                    >
                      <span className="watchlist-star active">&#9733;</span>
                      {sym}
                    </button>
                  ))}
                </div>
              </div>
            )}
            {renderedGroups.map((group) => (
              <div className="ticker-panel-group" key={group.label}>
                <div className="ticker-panel-group-label">{group.label}</div>
                <div className="ticker-panel-group-items">
                  {group.symbols.map((sym) => (
                    <button
                      key={sym}
                      className={`ticker-panel-item ${sym === selectedSymbol ? 'active' : ''}`}
                      onClick={() => handleSelectTicker(sym)}
                    >
                      {onToggleWatchlist && (
                        <span
                          className={`watchlist-star ${watchlist.includes(sym) ? 'active' : ''}`}
                          onClick={(e) => { e.stopPropagation(); onToggleWatchlist(sym); }}
                        >
                          {watchlist.includes(sym) ? '\u2733' : '\u2606'}
                        </span>
                      )}
                      {sym}
                    </button>
                  ))}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Search */}
      <div className="search-wrapper" ref={searchRef}>
        <svg className="search-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="11" cy="11" r="8" />
          <path d="M21 21l-4.35-4.35" />
        </svg>
        <input
          type="text"
          placeholder="Search..."
          value={query}
          onChange={(e) => handleSearch(e.target.value)}
          onFocus={() => results.length > 0 && setShowSearch(true)}
        />
        {showSearch && results.length > 0 && (
          <ul className="search-dropdown">
            {results.map((t) => (
              <li key={t.symbol} onClick={() => handlePick(t)}>
                <strong>{t.symbol}</strong> <span>{t.name}</span>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
