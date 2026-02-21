/**
 * Trade Simulator Module
 * Backtests a simple trade strategy across filtered chart days
 */

const TradeSimulator = (function() {

    /**
     * Check if simulation parameters are valid
     * @param {Object} params - { triggerBar, direction, targetPct, stopPct }
     * @returns {boolean}
     */
    function isValid(params) {
        return params &&
            params.triggerBar > 0 &&
            (params.direction === 'Long' || params.direction === 'Short') &&
            params.targetPct > 0 &&
            params.stopPct > 0;
    }

    /**
     * Simulate a trade on a single chart day
     * @param {Object} chartData - Chart data with bars array [timestamp, open, high, low, close]
     * @param {Object} params - { triggerBar, direction, targetPct, stopPct }
     * @returns {Object} { outcome: 'WIN'|'LOSS'|'SKIP', pnl: number, entry, target, stop }
     */
    function simulateOne(chartData, params) {
        const bars = chartData.bars;
        const triggerIndex = params.triggerBar - 1; // Convert to 0-indexed

        // Not enough bars for trigger
        if (triggerIndex >= bars.length) {
            return { outcome: 'SKIP', pnl: 0, reason: 'not enough bars' };
        }

        const triggerBar = bars[triggerIndex];
        const entry = triggerBar[4]; // close
        const high = triggerBar[2];
        const low = triggerBar[3];
        const range = high - low;

        // Zero-range trigger bar
        if (range <= 0) {
            return { outcome: 'SKIP', pnl: 0, reason: 'zero range', entry };
        }

        const targetOffset = (params.targetPct / 100) * range;
        const stopOffset = (params.stopPct / 100) * range;

        let targetPrice, stopPrice;

        if (params.direction === 'Long') {
            targetPrice = entry + targetOffset;
            stopPrice = entry - stopOffset;
        } else {
            targetPrice = entry - targetOffset;
            stopPrice = entry + stopOffset;
        }

        // Scan subsequent bars
        for (let i = triggerIndex + 1; i < bars.length; i++) {
            const barHigh = bars[i][2];
            const barLow = bars[i][3];

            let hitTarget, hitStop;

            if (params.direction === 'Long') {
                hitTarget = barHigh >= targetPrice;
                hitStop = barLow <= stopPrice;
            } else {
                hitTarget = barLow <= targetPrice;
                hitStop = barHigh >= stopPrice;
            }

            if (hitTarget && hitStop) {
                // Both hit in same bar — indecisive
                return { outcome: 'SKIP', pnl: 0, reason: 'both hit', entry, target: targetPrice, stop: stopPrice };
            }

            if (hitTarget) {
                return { outcome: 'WIN', pnl: targetOffset, entry, target: targetPrice, stop: stopPrice };
            }

            if (hitStop) {
                return { outcome: 'LOSS', pnl: -stopOffset, entry, target: targetPrice, stop: stopPrice };
            }
        }

        // Neither hit by end of day
        return { outcome: 'SKIP', pnl: 0, reason: 'end of day', entry, target: targetPrice, stop: stopPrice };
    }

    /**
     * Run simulation across all filtered charts
     * @param {Array} charts - Array of chart data objects
     * @param {Object} params - { triggerBar, direction, targetPct, stopPct }
     * @returns {Object} { wins, losses, skipped, decisive, winRate, totalPnL, trades[] }
     */
    function simulate(charts, params) {
        if (!isValid(params)) {
            return null;
        }

        const trades = [];
        let wins = 0;
        let losses = 0;
        let skipped = 0;
        let totalPnL = 0;

        for (const chart of charts) {
            const result = simulateOne(chart, params);
            trades.push({ key: chart.key, ...result });

            if (result.outcome === 'WIN') {
                wins++;
                totalPnL += result.pnl;
            } else if (result.outcome === 'LOSS') {
                losses++;
                totalPnL += result.pnl;
            } else {
                skipped++;
            }
        }

        const decisive = wins + losses;
        const winRate = decisive > 0 ? (wins / decisive) * 100 : 0;
        const avgPnL = decisive > 0 ? totalPnL / decisive : 0;

        return {
            wins,
            losses,
            skipped,
            decisive,
            winRate,
            avgPnL,
            trades
        };
    }

    // Public API
    return {
        isValid,
        simulate
    };
})();
