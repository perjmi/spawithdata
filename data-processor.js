/**
 * Data Processor Module
 * Handles loading and processing of OHLC data from multiple sources
 */

const DataProcessor = (function() {
    let rawData = null;
    let processedCharts = [];

    /**
     * Load OHLC data from JSON file
     * @param {function} progressCallback - Callback for progress updates (0-100)
     * @returns {Promise<Object>} Loaded data
     */
    async function loadData(progressCallback) {
        try {
            progressCallback && progressCallback(10, 'Fetching data file...');

            const response = await fetch('data/ohlc_data.json');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            progressCallback && progressCallback(50, 'Parsing JSON...');

            rawData = await response.json();

            progressCallback && progressCallback(80, 'Processing charts...');

            // Pre-process charts for filtering
            processCharts();

            progressCallback && progressCallback(100, 'Complete!');

            return rawData;
        } catch (error) {
            console.error('Error loading data:', error);
            throw error;
        }
    }

    /**
     * Process raw data into chart entries for filtering
     */
    function processCharts() {
        processedCharts = [];

        if (!rawData || !rawData.sources) return;

        for (const source of rawData.sources) {
            for (const day of source.tradingDays) {
                // Create base chart entry (5min is the base frequency)
                processedCharts.push({
                    source: source.name,
                    timezone: source.timezone,
                    tradingHours: source.tradingHours,
                    date: day.date,
                    gapDirection: day.gapDirection,
                    gapSizeClass: day.gapSizeClass,
                    openAbovePrevHigh: day.openAbovePrevHigh,
                    closeBelowPrevLow: day.closeBelowPrevLow,
                    prevClose: day.prevClose,
                    prevHigh: day.prevHigh,
                    prevLow: day.prevLow,
                    bars: day.bars,
                    barDirections: day.barDirections,
                    bodyRatios: day.bodyRatios || []
                });
            }
        }
    }

    /**
     * Get all unique source names
     * @returns {Array<string>} Source names
     */
    function getSources() {
        if (!rawData || !rawData.sources) return [];
        return rawData.sources.map(s => s.name);
    }

    /**
     * Get source metadata (timezone, trading hours)
     * @param {string} sourceName - Source name
     * @returns {Object} Source metadata
     */
    function getSourceMetadata(sourceName) {
        if (!rawData || !rawData.sources) return null;
        const source = rawData.sources.find(s => s.name === sourceName);
        if (!source) return null;
        return {
            timezone: source.timezone,
            tradingHours: source.tradingHours
        };
    }

    /**
     * Get maximum bar count across all days
     * @returns {number} Max bar count
     */
    function getMaxBarCount() {
        let maxBars = 0;
        for (const chart of processedCharts) {
            if (chart.barDirections && chart.barDirections.length > maxBars) {
                maxBars = chart.barDirections.length;
            }
        }
        return Math.min(maxBars, 120); // Cap at 120 for UI
    }

    /**
     * Get trading day data for rendering
     * @param {string} sourceName - Source name
     * @param {string} date - Date string (YYYYMMDD)
     * @param {string} frequency - Frequency ('5min', '10min', '15min')
     * @param {number} maxBars - Maximum bars to include (36 or 999)
     * @returns {Object} Chart data with bars and metadata
     */
    function getTradingDayData(sourceName, date, frequency, maxBars) {
        // Find the chart entry
        const entry = processedCharts.find(
            c => c.source === sourceName && c.date === date
        );

        if (!entry) return null;

        // Get base 5min bars
        let bars = [...entry.bars];

        // Aggregate to target frequency
        bars = ChartRenderer.aggregateToFrequency(bars, frequency);

        // Limit bars if needed
        if (maxBars < 999 && bars.length > maxBars) {
            bars = bars.slice(0, maxBars);
        }

        // Calculate bar directions and body ratios for the aggregated frequency
        const barDirections = ChartRenderer.calculateBarDirections(bars);
        const bodyRatios = ChartRenderer.calculateBodyRatios(bars);

        return {
            source: entry.source,
            timezone: entry.timezone,
            tradingHours: entry.tradingHours,
            date: entry.date,
            frequency,
            maxBars,
            gapDirection: entry.gapDirection,
            gapSizeClass: entry.gapSizeClass,
            openAbovePrevHigh: entry.openAbovePrevHigh,
            closeBelowPrevLow: entry.closeBelowPrevLow,
            prevClose: entry.prevClose,
            prevHigh: entry.prevHigh,
            prevLow: entry.prevLow,
            bars,
            barDirections,
            bodyRatios
        };
    }

    /**
     * Generate all chart combinations based on filters
     * @param {Object} filters - Filter criteria
     * @returns {Array} Filtered chart entries
     */
    function getFilteredCharts(filters) {
        const {
            sources = [],
            frequencies = ['5min'],
            barsOptions = [999],
            gapDirections = [],
            gapSizeClasses = [],
            prevDayFilters = [],
            barFilters = []
        } = filters;

        const results = [];

        for (const entry of processedCharts) {
            // Source filter
            if (sources.length > 0 && !sources.includes(entry.source)) {
                continue;
            }

            // Gap direction filter
            if (gapDirections.length > 0 && !gapDirections.includes(entry.gapDirection)) {
                continue;
            }

            // Gap size filter
            if (gapSizeClasses.length > 0 && !gapSizeClasses.includes(entry.gapSizeClass)) {
                continue;
            }

            // Previous day comparison filters
            if (prevDayFilters.length > 0) {
                let matchesPrevDay = true;
                for (const filter of prevDayFilters) {
                    if (filter === 'open_above_prev_high' && entry.openAbovePrevHigh !== true) {
                        matchesPrevDay = false;
                        break;
                    }
                    if (filter === 'close_below_prev_low' && entry.closeBelowPrevLow !== true) {
                        matchesPrevDay = false;
                        break;
                    }
                }
                if (!matchesPrevDay) continue;
            }

            // Generate combinations for each frequency and bars option
            for (const freq of frequencies) {
                for (const barsOpt of barsOptions) {
                    // Get aggregated data for this frequency
                    const chartData = getTradingDayData(
                        entry.source,
                        entry.date,
                        freq,
                        barsOpt
                    );

                    if (!chartData || chartData.bars.length < 5) {
                        continue;
                    }

                    // Bar direction and body ratio filters (must match ALL)
                    if (barFilters.length > 0) {
                        let matchesBarFilters = true;
                        for (const bf of barFilters) {
                            const barIndex = bf.bar - 1; // Convert to 0-indexed
                            if (barIndex >= chartData.barDirections.length) {
                                matchesBarFilters = false;
                                break;
                            }
                            // Check direction
                            if (chartData.barDirections[barIndex] !== bf.direction) {
                                matchesBarFilters = false;
                                break;
                            }
                            // Check body ratio if specified
                            if (bf.bodyRatio && bf.bodyRatio !== 'any') {
                                if (barIndex >= chartData.bodyRatios.length) {
                                    matchesBarFilters = false;
                                    break;
                                }
                                if (chartData.bodyRatios[barIndex] !== bf.bodyRatio) {
                                    matchesBarFilters = false;
                                    break;
                                }
                            }
                        }
                        if (!matchesBarFilters) continue;
                    }

                    results.push({
                        ...chartData,
                        key: `${entry.source}-${entry.date}-${freq}-${barsOpt}`
                    });
                }
            }
        }

        return results;
    }

    /**
     * Get metadata about the loaded data
     * @returns {Object} Metadata
     */
    function getMetadata() {
        if (!rawData || !rawData.metadata) return null;
        return rawData.metadata;
    }

    /**
     * Get total number of trading days
     * @returns {number} Total trading days
     */
    function getTotalTradingDays() {
        return processedCharts.length;
    }

    // Public API
    return {
        loadData,
        getSources,
        getSourceMetadata,
        getMaxBarCount,
        getTradingDayData,
        getFilteredCharts,
        getMetadata,
        getTotalTradingDays
    };
})();
