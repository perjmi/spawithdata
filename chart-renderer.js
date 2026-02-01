/**
 * Chart Renderer Module
 * Wrapper for TradingView's lightweight-charts library
 */

const ChartRenderer = (function() {
    // Store active charts for cleanup
    const activeCharts = new Map();

    /**
     * Get timezone offset in seconds for a given timestamp and timezone
     * @param {number} timestampMs - UTC timestamp in milliseconds
     * @param {string} timezone - IANA timezone string (e.g., 'Europe/London')
     * @returns {number} Offset in seconds to add to UTC to get local time
     */
    function getTimezoneOffset(timestampMs, timezone) {
        const date = new Date(timestampMs);
        // Get UTC time string
        const utcStr = date.toLocaleString('en-US', { timeZone: 'UTC' });
        // Get local time string in target timezone
        const localStr = date.toLocaleString('en-US', { timeZone: timezone });
        // Parse both and calculate difference
        const utcDate = new Date(utcStr);
        const localDate = new Date(localStr);
        return (localDate - utcDate) / 1000; // Return offset in seconds
    }

    /**
     * Create a candlestick chart in the specified container
     * @param {string} containerId - DOM element ID for the chart container
     * @param {Array} ohlcData - Array of [timestamp_ms, open, high, low, close]
     * @param {Object} options - Chart options (including timezone)
     * @returns {Object} Chart instance
     */
    function createChart(containerId, ohlcData, options = {}) {
        const container = document.getElementById(containerId);
        if (!container) {
            console.error(`Container ${containerId} not found`);
            return null;
        }

        // Destroy existing chart if any
        destroyChart(containerId);

        // Default chart options
        const chartOptions = {
            width: container.clientWidth,
            height: options.height || 300,
            layout: {
                background: { type: 'solid', color: '#131722' },
                textColor: '#d1d4dc',
            },
            grid: {
                vertLines: { color: '#1e222d' },
                horzLines: { color: '#1e222d' },
            },
            crosshair: {
                mode: LightweightCharts.CrosshairMode.Normal,
            },
            rightPriceScale: {
                borderColor: '#2B2B43',
            },
            timeScale: {
                borderColor: '#2B2B43',
                timeVisible: true,
                secondsVisible: false,
            },
            ...options.chartOptions
        };

        const chart = LightweightCharts.createChart(container, chartOptions);

        // Add candlestick series
        const candlestickSeries = chart.addCandlestickSeries({
            upColor: '#26a69a',
            downColor: '#ef5350',
            borderDownColor: '#ef5350',
            borderUpColor: '#26a69a',
            wickDownColor: '#ef5350',
            wickUpColor: '#26a69a',
        });

        // Convert data to lightweight-charts format
        // Apply timezone offset if specified to display local time
        const timezone = options.timezone;
        const formattedData = ohlcData.map(bar => {
            let timeInSeconds = bar[0] / 1000;
            // Adjust for timezone if specified
            if (timezone) {
                const offset = getTimezoneOffset(bar[0], timezone);
                timeInSeconds += offset;
            }
            return {
                time: timeInSeconds,
                open: bar[1],
                high: bar[2],
                low: bar[3],
                close: bar[4]
            };
        });

        candlestickSeries.setData(formattedData);

        // Fit content to view
        chart.timeScale().fitContent();

        // Handle resize
        const resizeObserver = new ResizeObserver(entries => {
            for (const entry of entries) {
                const { width, height } = entry.contentRect;
                chart.applyOptions({ width, height: options.height || 300 });
            }
        });
        resizeObserver.observe(container);

        // Store chart reference
        activeCharts.set(containerId, { chart, resizeObserver });

        return chart;
    }

    /**
     * Destroy a chart and clean up resources
     * @param {string} containerId - DOM element ID of the chart container
     */
    function destroyChart(containerId) {
        const chartData = activeCharts.get(containerId);
        if (chartData) {
            chartData.resizeObserver.disconnect();
            chartData.chart.remove();
            activeCharts.delete(containerId);
        }
    }

    /**
     * Destroy all active charts
     */
    function destroyAllCharts() {
        for (const containerId of activeCharts.keys()) {
            destroyChart(containerId);
        }
    }

    /**
     * Aggregate 5-minute bars to a higher frequency
     * @param {Array} bars5min - Array of [timestamp_ms, open, high, low, close]
     * @param {string} targetFreq - Target frequency ('5min', '10min', '15min')
     * @returns {Array} Aggregated bars
     */
    function aggregateToFrequency(bars5min, targetFreq) {
        if (targetFreq === '5min') {
            return bars5min;
        }

        const periodMinutes = targetFreq === '10min' ? 10 : 15;
        const barsPerPeriod = periodMinutes / 5;

        const result = [];
        for (let i = 0; i < bars5min.length; i += barsPerPeriod) {
            const chunk = bars5min.slice(i, i + barsPerPeriod);
            if (chunk.length === 0) continue;

            const aggregated = [
                chunk[0][0], // timestamp of first bar
                chunk[0][1], // open of first bar
                Math.max(...chunk.map(b => b[2])), // highest high
                Math.min(...chunk.map(b => b[3])), // lowest low
                chunk[chunk.length - 1][4] // close of last bar
            ];
            result.push(aggregated);
        }

        return result;
    }

    /**
     * Calculate bar directions after aggregation
     * @param {Array} bars - Array of [timestamp_ms, open, high, low, close]
     * @returns {Array} Array of directions ('UP', 'DOWN', 'FLAT')
     */
    function calculateBarDirections(bars) {
        return bars.map(bar => {
            const open = bar[1];
            const close = bar[4];
            if (close > open) return 'UP';
            if (close < open) return 'DOWN';
            return 'FLAT';
        });
    }

    /**
     * Calculate body-to-range ratios after aggregation
     * @param {Array} bars - Array of [timestamp_ms, open, high, low, close]
     * @returns {Array} Array of ratio categories ('<25%', '25-50%', '50-75%', '>75%')
     */
    function calculateBodyRatios(bars) {
        return bars.map(bar => {
            const open = bar[1];
            const high = bar[2];
            const low = bar[3];
            const close = bar[4];

            const range = high - low;
            if (range <= 0) return '<25%';

            const body = Math.abs(close - open);
            const ratio = (body / range) * 100;

            if (ratio < 25) return '<25%';
            if (ratio < 50) return '25-50%';
            if (ratio < 75) return '50-75%';
            return '>75%';
        });
    }

    // Public API
    return {
        createChart,
        destroyChart,
        destroyAllCharts,
        aggregateToFrequency,
        calculateBarDirections,
        calculateBodyRatios
    };
})();
