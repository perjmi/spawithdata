/**
 * Main Application Controller
 * Handles login, filter management, and chart rendering
 */

// Global state
let barFilters = []; // Array of {bar: number, direction: string}
const MAX_CHARTS = 50;
const MAX_BAR_NUMBER = 120;

// Credentials
const VALID_USERNAME = 'tradertom';
const VALID_PASSWORD = 'tradingwithcharts';

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', init);

function init() {
    setupLogin();
    setupEventListeners();
}

// Login functionality
function setupLogin() {
    const loginForm = document.getElementById('login-form');
    const loginScreen = document.getElementById('login-screen');
    const loadingScreen = document.getElementById('loading-screen');
    const mainApp = document.getElementById('main-app');
    const loginError = document.getElementById('login-error');

    // Check if already logged in (session storage)
    if (sessionStorage.getItem('authenticated') === 'true') {
        loginScreen.style.display = 'none';
        showLoadingAndLoadData();
    }

    loginForm.addEventListener('submit', (e) => {
        e.preventDefault();
        const username = document.getElementById('username').value;
        const password = document.getElementById('password').value;

        if (username === VALID_USERNAME && password === VALID_PASSWORD) {
            sessionStorage.setItem('authenticated', 'true');
            loginScreen.style.display = 'none';
            loginError.textContent = '';
            showLoadingAndLoadData();
        } else {
            loginError.textContent = 'Invalid username or password';
        }
    });
}

// Show loading screen and load data
async function showLoadingAndLoadData() {
    const loadingScreen = document.getElementById('loading-screen');
    const mainApp = document.getElementById('main-app');
    const progressFill = document.getElementById('progress-fill');
    const loadingStatus = document.getElementById('loading-status');

    loadingScreen.style.display = 'flex';

    try {
        await DataProcessor.loadData((progress, status) => {
            progressFill.style.width = `${progress}%`;
            loadingStatus.textContent = status;
        });

        // Small delay for visual feedback
        await new Promise(resolve => setTimeout(resolve, 300));

        loadingScreen.style.display = 'none';
        mainApp.style.display = 'flex';

        // Initialize filters and apply
        populateSourceFilter();
        populateBarNumberDropdown();
        updateGalleryColumns();
        applyFilters();

    } catch (error) {
        loadingStatus.textContent = `Error loading data: ${error.message}`;
        progressFill.style.background = '#e74c3c';
    }
}

// Setup event listeners
function setupEventListeners() {
    document.getElementById('apply-filters').addEventListener('click', applyFilters);
    document.getElementById('clear-filters').addEventListener('click', clearFilters);
    document.getElementById('add-bar-filter').addEventListener('click', addBarFilter);
    document.getElementById('charts-per-row').addEventListener('change', updateGalleryColumns);

    // Export buttons
    document.getElementById('export-markdown').addEventListener('click', () => Exporter.exportToMarkdown());
    document.getElementById('export-pptx').addEventListener('click', () => Exporter.exportToPowerPoint());
}

// Update gallery columns based on selection
function updateGalleryColumns() {
    const cols = document.getElementById('charts-per-row').value;
    const gallery = document.getElementById('gallery');

    gallery.classList.remove('cols-1', 'cols-2', 'cols-3', 'cols-4', 'cols-5');
    gallery.classList.add(`cols-${cols}`);
}

// Populate source filter checkboxes from data
function populateSourceFilter() {
    const container = document.getElementById('instrument-filter');
    const sources = DataProcessor.getSources();

    container.innerHTML = sources.map(source => {
        const meta = DataProcessor.getSourceMetadata(source);
        const tzLabel = meta ? ` (${meta.timezone.split('/')[1]})` : '';
        return `<label><input type="checkbox" value="${source}" checked> ${source}${tzLabel}</label>`;
    }).join('');
}

// Populate bar number dropdown
function populateBarNumberDropdown() {
    const select = document.getElementById('bar-number-select');
    select.innerHTML = '<option value="">Select bar #</option>';

    const maxBar = DataProcessor.getMaxBarCount();

    for (let i = 1; i <= maxBar; i++) {
        const option = document.createElement('option');
        option.value = i;
        option.textContent = `Bar ${i}`;
        select.appendChild(option);
    }
}

// Add a bar filter
function addBarFilter() {
    const barSelect = document.getElementById('bar-number-select');
    const directionSelect = document.getElementById('bar-direction-select');

    const barNum = parseInt(barSelect.value);
    const direction = directionSelect.value;

    if (!barNum) return;

    // Check if this bar filter already exists
    const exists = barFilters.some(f => f.bar === barNum);
    if (exists) {
        // Update existing filter
        barFilters = barFilters.map(f => f.bar === barNum ? {bar: barNum, direction} : f);
    } else {
        // Add new filter
        barFilters.push({bar: barNum, direction});
    }

    // Sort by bar number
    barFilters.sort((a, b) => a.bar - b.bar);

    renderBarFilters();
    barSelect.value = '';
}

// Remove a bar filter
function removeBarFilter(barNum) {
    barFilters = barFilters.filter(f => f.bar !== barNum);
    renderBarFilters();
}

// Render bar filter chips
function renderBarFilters() {
    const container = document.getElementById('bar-filters-container');

    if (barFilters.length === 0) {
        container.innerHTML = '<p style="color: #7f8c8d; font-size: 0.85em; padding: 8px;">No bar filters (showing all)</p>';
        return;
    }

    container.innerHTML = barFilters.map(f => `
        <div class="bar-filter-item">
            <span>Bar ${f.bar} = <strong>${f.direction}</strong></span>
            <button class="remove-btn" onclick="removeBarFilter(${f.bar})">Remove</button>
        </div>
    `).join('');
}

// Get checked values for a filter group
function getCheckedValues(filterName) {
    const checkboxes = document.querySelectorAll(`[data-filter="${filterName}"] input[type="checkbox"]:checked`);
    return Array.from(checkboxes).map(cb => cb.value);
}

// Format date from YYYYMMDD to readable format
function formatDate(dateStr) {
    if (!dateStr) return 'Unknown';
    const str = dateStr.toString();
    if (str.length !== 8) return dateStr;
    return `${str.slice(0,4)}-${str.slice(4,6)}-${str.slice(6,8)}`;
}

// Get source badge class based on timezone
function getSourceBadgeClass(source) {
    const meta = DataProcessor.getSourceMetadata(source);
    if (!meta) return '';

    if (meta.timezone === 'America/New_York') {
        return 'source-us';
    } else if (meta.timezone === 'Europe/London') {
        return 'source-eu';
    }
    return '';
}

// Create chart card HTML
function createChartCard(chartData, index) {
    const containerId = `chart-${index}`;
    const gapClass = chartData.gapDirection === 'GAP UP' ? 'gap-up' :
                     chartData.gapDirection === 'GAP DOWN' ? 'gap-down' : '';

    const sourceClass = getSourceBadgeClass(chartData.source);

    // Create bar directions display (first 20 bars)
    let barDirsHtml = '';
    if (chartData.barDirections && chartData.barDirections.length > 0) {
        const displayBars = chartData.barDirections.slice(0, 20);
        barDirsHtml = displayBars.map((dir, i) => {
            const cls = dir === 'UP' ? 'bar-up' :
                        dir === 'DOWN' ? 'bar-down' : 'bar-flat';
            return `<span class="${cls}">${i + 1}:${dir.charAt(0)}</span>`;
        }).join('');
        if (chartData.barDirections.length > 20) {
            barDirsHtml += `<span>... +${chartData.barDirections.length - 20} more</span>`;
        }
    }

    // Previous day comparison indicators
    let prevDayHtml = '';
    if (chartData.openAbovePrevHigh === true) {
        prevDayHtml += '<span class="prev-day-indicator open-above">O>PH</span>';
    }
    if (chartData.closeBelowPrevLow === true) {
        prevDayHtml += '<span class="prev-day-indicator close-below">C<PL</span>';
    }

    // Bars display
    const barsDisplay = chartData.maxBars === 999 ? 'Full day' : `${chartData.maxBars} bars`;

    // Timezone indicator
    const tzShort = chartData.timezone === 'America/New_York' ? 'NY' : 'LON';

    return `
        <div class="chart-card">
            <div class="chart-info">
                <h3>${chartData.source} - ${formatDate(chartData.date)}</h3>
                <div class="chart-meta">
                    <span class="${sourceClass}">${chartData.source}</span>
                    <span class="tz-badge">${tzShort}</span>
                    <span>${chartData.frequency}</span>
                    <span>${barsDisplay}</span>
                    <span class="${gapClass}">${chartData.gapDirection}</span>
                    <span>${chartData.gapSizeClass}</span>
                    ${prevDayHtml}
                </div>
            </div>
            ${barDirsHtml ? `<div class="bar-directions">${barDirsHtml}</div>` : ''}
            <div class="chart-container" id="${containerId}"></div>
        </div>
    `;
}

// Apply filters and render gallery
function applyFilters() {
    // Destroy existing charts
    ChartRenderer.destroyAllCharts();

    // Get filter values
    const filters = {
        sources: getCheckedValues('instrument'),
        frequencies: getCheckedValues('freq'),
        barsOptions: getCheckedValues('bars').map(v => parseInt(v)),
        gapDirections: getCheckedValues('gap_direction'),
        gapSizeClasses: getCheckedValues('gap_size_class'),
        prevDayFilters: getCheckedValues('prev_day'),
        barFilters: barFilters
    };

    // Get filtered charts
    const filteredCharts = DataProcessor.getFilteredCharts(filters);
    const toDisplay = filteredCharts.slice(0, MAX_CHARTS);

    // Update counts
    document.getElementById('visible-count').textContent = toDisplay.length;
    document.getElementById('total-count').textContent = filteredCharts.length;

    // Update exporter with current charts
    Exporter.setCharts(toDisplay);

    const gallery = document.getElementById('gallery');

    if (toDisplay.length === 0) {
        gallery.innerHTML = '<p class="no-results">No charts match the selected filters.</p>';
        return;
    }

    // Render chart cards
    gallery.innerHTML = toDisplay.map((chart, index) => createChartCard(chart, index)).join('');

    // Create charts after DOM update
    requestAnimationFrame(() => {
        toDisplay.forEach((chartData, index) => {
            const containerId = `chart-${index}`;
            ChartRenderer.createChart(containerId, chartData.bars);
        });
    });
}

// Clear all filters
function clearFilters() {
    // Check all checkboxes
    document.querySelectorAll('.checkbox-group input[type="checkbox"]').forEach(cb => {
        // For prev_day filters, uncheck them (they are opt-in)
        if (cb.closest('[data-filter="prev_day"]')) {
            cb.checked = false;
        } else {
            cb.checked = true;
        }
    });

    // Clear bar filters
    barFilters = [];
    renderBarFilters();

    // Re-apply filters
    applyFilters();
}
