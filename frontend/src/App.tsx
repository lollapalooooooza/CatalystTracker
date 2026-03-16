import { useState, useEffect, useCallback, useRef } from 'react';
import axios from 'axios';
import StockSelector from './components/StockSelector';
import CandlestickChart from './components/CandlestickChart';
import NewsPanel from './components/NewsPanel';
import NewsCategoryPanel from './components/NewsCategoryPanel';
import RangeAnalysisPanel from './components/RangeAnalysisPanel';
import RangeQueryPopup from './components/RangeQueryPopup';
import RangeNewsPanel from './components/RangeNewsPanel';
import SimilarDaysPanel from './components/SimilarDaysPanel';
import PredictionPanel from './components/PredictionPanel';
import ToastContainer from './components/Toast';
import './App.css';

interface RangeSelection {
  startDate: string;
  endDate: string;
  priceChange?: number;
  popupX?: number;
  popupY?: number;
}

interface ArticleSelection {
  newsId: string;
  date: string;
}

function App() {
  const [activeTickers, setActiveTickers] = useState<string[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState('');
  const [hoveredDate, setHoveredDate] = useState<string | null>(null);
  const [selectedRange, setSelectedRange] = useState<RangeSelection | null>(null);
  const [rangeQuestion, setRangeQuestion] = useState<string | null>(null);
  const [predView, setPredView] = useState<'prediction' | 'ask'>('prediction');
  const [selectedDay, setSelectedDay] = useState<string | null>(null);
  const [selectedArticle, setSelectedArticle] = useState<ArticleSelection | null>(null);

  // Locked article state (click-to-lock)
  const [lockedArticle, setLockedArticle] = useState<ArticleSelection | null>(null);

  // News category filter
  const [activeCategory, setActiveCategory] = useState<string | null>(null);
  const [activeCategoryIds, setActiveCategoryIds] = useState<string[]>([]);
  const [activeCategoryColor, setActiveCategoryColor] = useState<string | null>(null);

  // Watchlist
  const [watchlist, setWatchlist] = useState<string[]>(() => {
    try { return JSON.parse(localStorage.getItem('ct-watchlist') || '[]'); } catch { return []; }
  });

  // Chart area ref for popup positioning
  const chartAreaRef = useRef<HTMLDivElement>(null);
  const [chartRect, setChartRect] = useState<DOMRect | undefined>(undefined);

  // Persist watchlist
  useEffect(() => {
    localStorage.setItem('ct-watchlist', JSON.stringify(watchlist));
  }, [watchlist]);

  const toggleWatchlist = useCallback((sym: string) => {
    setWatchlist(prev => prev.includes(sym) ? prev.filter(s => s !== sym) : [...prev, sym]);
  }, []);

  // Keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.target as HTMLElement).tagName === 'INPUT' || (e.target as HTMLElement).tagName === 'TEXTAREA') return;
      if (e.key === 'Escape') {
        if (selectedRange) { setSelectedRange(null); setRangeQuestion(null); }
        else if (lockedArticle) { setLockedArticle(null); setSelectedArticle(null); }
        else if (selectedDay) { setSelectedDay(null); }
      }
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
        e.preventDefault();
        window.dispatchEvent(new CustomEvent('chart-navigate', { detail: { direction: e.key === 'ArrowLeft' ? -1 : 1 } }));
      }
    }
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [selectedRange, lockedArticle, selectedDay]);

  useEffect(() => {
    axios
      .get('/api/stocks')
      .then((res) => {
        const tickers = res.data
          .filter((t: any) => t.last_ohlc_fetch)
          .map((t: any) => t.symbol);
        setActiveTickers(tickers);
        if (tickers.length > 0 && !selectedSymbol) {
          setSelectedSymbol(tickers[0]);
        }
      })
      .catch(console.error);
  }, []);

  // Update chartRect when range is selected (for popup positioning)
  useEffect(() => {
    if (selectedRange && chartAreaRef.current) {
      setChartRect(chartAreaRef.current.getBoundingClientRect());
    }
  }, [selectedRange]);

  const handleHover = useCallback(
    (date: string | null) => {
      if (!lockedArticle) {
        setHoveredDate(date);
      }
    },
    [lockedArticle]
  );

  const handleRangeSelect = useCallback((range: RangeSelection | null) => {
    setSelectedRange(range);
    setRangeQuestion(null);
    setPredView(range ? 'ask' : 'prediction');
    if (range) {
      setSelectedDay(null);
      setSelectedArticle(null);
      setLockedArticle(null);
    }
  }, []);

  const handleArticleSelect = useCallback((article: ArticleSelection | null) => {
    if (article === null) {
      // Unlock
      setLockedArticle(null);
      setSelectedArticle(null);
      return;
    }
    // Toggle: click same dot → unlock, different dot → lock new
    setLockedArticle((prev) => {
      if (prev && prev.newsId === article.newsId) {
        // Unlock
        setSelectedArticle(null);
        return null;
      }
      // Lock new
      setSelectedArticle(article);
      setSelectedRange(null);
      setRangeQuestion(null);
      setSelectedDay(null);
      setHoveredDate(article.date);
      return article;
    });
  }, []);

  const handleDayClick = useCallback((date: string) => {
    setSelectedDay(date);
    setSelectedRange(null);
    setRangeQuestion(null);
    setSelectedArticle(null);
    setLockedArticle(null);
  }, []);

  const handleRangeAsk = useCallback((question: string) => {
    setRangeQuestion(question);
  }, []);

  const handleCategoryChange = useCallback((category: string | null, articleIds: string[], color?: string) => {
    setActiveCategory(category);
    setActiveCategoryIds(articleIds);
    setActiveCategoryColor(color ?? null);
  }, []);

  function handleSelectSymbol(symbol: string) {
    setSelectedSymbol(symbol);
    setHoveredDate(null);
    setSelectedRange(null);
    setRangeQuestion(null);
    setPredView('prediction');
    setSelectedDay(null);
    setSelectedArticle(null);
    setLockedArticle(null);
    setActiveCategory(null);
    setActiveCategoryIds([]);
    setActiveCategoryColor(null);
  }

  function handleAddTicker(symbol: string) {
    if (!activeTickers.includes(symbol)) {
      setActiveTickers((prev) => [...prev, symbol]);
      axios.post('/api/stocks', { symbol }).catch(console.error);
    }
  }

  // Effective date for NewsPanel: locked takes priority
  const effectiveDate = lockedArticle?.date ?? hoveredDate;
  const isLocked = lockedArticle !== null;

  // Right panel priority: rangeQuestion > rangeNews > selectedDay > default NewsPanel
  function renderRightPanel() {
    if (selectedRange && rangeQuestion) {
      return (
        <RangeAnalysisPanel
          symbol={selectedSymbol}
          startDate={selectedRange.startDate}
          endDate={selectedRange.endDate}
          question={rangeQuestion}
          onClear={() => {
            setSelectedRange(null);
            setRangeQuestion(null);
          }}
        />
      );
    }
    if (selectedRange && !rangeQuestion) {
      return (
        <RangeNewsPanel
          symbol={selectedSymbol}
          startDate={selectedRange.startDate}
          endDate={selectedRange.endDate}
          priceChange={selectedRange.priceChange}
          onClose={() => setSelectedRange(null)}
        />
      );
    }
    if (selectedDay) {
      return (
        <SimilarDaysPanel
          symbol={selectedSymbol}
          date={selectedDay}
          onClose={() => setSelectedDay(null)}
        />
      );
    }
    return (
      <>
        <NewsPanel
          symbol={selectedSymbol}
          hoveredDate={effectiveDate}
          onFindSimilar={(_newsId: string) => {
            if (effectiveDate) handleDayClick(effectiveDate);
          }}
          highlightedNewsId={selectedArticle?.newsId || null}
          isLocked={isLocked}
          onUnlock={() => {
            setLockedArticle(null);
            setSelectedArticle(null);
          }}
          highlightedCategoryIds={activeCategoryIds.length > 0 ? activeCategoryIds : undefined}
        />
      </>
    );
  }

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-gradient-line" />
        <div className="header-content header-content-minimal">
          <div className="header-left">
            <StockSelector
              activeTickers={activeTickers}
              selectedSymbol={selectedSymbol}
              onSelect={handleSelectSymbol}
              onAdd={handleAddTicker}
              watchlist={watchlist}
              onToggleWatchlist={toggleWatchlist}
            />
            {selectedRange ? (
              <div className="header-range-pill">
                <span className="header-range-label">Range</span>
                <span className="header-range-dates">{selectedRange.startDate} ~ {selectedRange.endDate}</span>
                <span className={`range-change ${(selectedRange.priceChange ?? 0) >= 0 ? 'up' : 'down'}`}>
                  {(selectedRange.priceChange ?? 0) >= 0 ? '+' : ''}
                  {(selectedRange.priceChange ?? 0).toFixed(2)}%
                </span>
              </div>
            ) : null}
          </div>
          <div className="header-right" data-mode={predView}>
            <div className="header-mode-switch">
              <button className={`header-mode-btn ${predView === 'prediction' ? 'active' : ''}`} onClick={() => setPredView('prediction')}>
                Prediction
              </button>
              <button className={`header-mode-btn ${predView === 'ask' ? 'active' : ''}`} onClick={() => selectedRange && setPredView('ask')} disabled={!selectedRange}>
                AI Question
              </button>
            </div>
          </div>
        </div>
      </header>

      <main className="app-main">
        <div className="chart-area" ref={chartAreaRef}>
          {selectedSymbol ? (
            <>
              <CandlestickChart
                symbol={selectedSymbol}
                lockedNewsId={lockedArticle?.newsId ?? null}
                highlightedArticleIds={activeCategoryIds.length > 0 ? activeCategoryIds : null}
                highlightColor={activeCategoryColor}
                selectedRange={selectedRange}
                onHover={handleHover}
                onRangeSelect={handleRangeSelect}
                onArticleSelect={handleArticleSelect}
                onDayClick={handleDayClick}
              />
              {selectedRange && !rangeQuestion && (
                <RangeQueryPopup
                  range={selectedRange}
                  chartRect={chartRect}
                  onAsk={handleRangeAsk}
                  onClose={() => setSelectedRange(null)}
                />
              )}
            </>
          ) : (
            <div className="chart-placeholder">Select a ticker to view the chart</div>
          )}
        </div>
        {selectedSymbol && (
          <div className="prediction-area">
            {predView === 'ask' && selectedRange ? (
              rangeQuestion ? (
                <RangeAnalysisPanel
                  symbol={selectedSymbol}
                  startDate={selectedRange.startDate}
                  endDate={selectedRange.endDate}
                  question={rangeQuestion}
                  onClear={() => {
                    setSelectedRange(null);
                    setRangeQuestion(null);
                    setPredView('prediction');
                  }}
                />
              ) : (
                <div className="pred-ask-panel">
                  <div className="pred-ask-header">
                    <div>
                      <div className="pred-ask-title">Ask AI about selected range</div>
                      <div className="pred-ask-meta pred-ask-meta-mono">{selectedRange.startDate} ~ {selectedRange.endDate}</div>
                    </div>
                    <span className={`range-change ${(selectedRange.priceChange ?? 0) >= 0 ? 'up' : 'down'}`}>
                      {(selectedRange.priceChange ?? 0) >= 0 ? '+' : ''}
                      {(selectedRange.priceChange ?? 0).toFixed(2)}%
                    </span>
                  </div>
                  <div className="pred-ask-presets">
                    {['What\'s driving the price movement?', 'Summarize key news in this period', 'What are the bull/bear factors?'].map((q) => (
                      <button key={q} className="pred-ask-preset" onClick={() => handleRangeAsk(q)}>
                        {q}
                      </button>
                    ))}
                  </div>
                </div>
              )
            ) : (
              <PredictionPanel symbol={selectedSymbol} />
            )}
          </div>
        )}
        <div className="news-area">
          {selectedSymbol && (
            <NewsCategoryPanel
              symbol={selectedSymbol}
              activeCategory={activeCategory}
              onCategoryChange={handleCategoryChange}
            />
          )}
          {renderRightPanel()}
        </div>
      </main>
      <ToastContainer />
    </div>
  );
}

export default App;
