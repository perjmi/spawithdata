/**
 * Export Module
 * Handles exporting charts to Markdown and PowerPoint formats
 */

const Exporter = {
    // Store current filtered charts for export
    currentCharts: [],

    // Set charts available for export
    setCharts(charts) {
        // Sort chronologically by date
        this.currentCharts = [...charts].sort((a, b) => {
            const dateA = parseInt(a.date);
            const dateB = parseInt(b.date);
            return dateA - dateB;
        });
    },

    // Convert a chart container to a canvas image
    async chartToImage(containerId) {
        const container = document.getElementById(containerId);
        if (!container) return null;

        // Find the canvas within the chart container
        const canvas = container.querySelector('canvas');
        if (!canvas) return null;

        // Return the canvas data URL
        return canvas.toDataURL('image/png');
    },

    // Format date from YYYYMMDD to readable format
    formatDate(dateStr) {
        if (!dateStr) return 'Unknown';
        const str = dateStr.toString();
        if (str.length !== 8) return dateStr;
        return `${str.slice(0, 4)}-${str.slice(4, 6)}-${str.slice(6, 8)}`;
    },

    // Generate Markdown export
    async exportToMarkdown() {
        if (this.currentCharts.length === 0) {
            alert('No charts to export. Apply filters first.');
            return;
        }

        const showProgress = this.showExportProgress('Generating Markdown...');

        try {
            let markdown = `# Chart Export\n\n`;
            markdown += `**Generated:** ${new Date().toISOString()}\n\n`;
            markdown += `**Total Charts:** ${this.currentCharts.length}\n\n`;
            markdown += `---\n\n`;

            for (let i = 0; i < this.currentCharts.length; i++) {
                const chart = this.currentCharts[i];
                const containerId = `chart-${i}`;

                showProgress(`Processing chart ${i + 1} of ${this.currentCharts.length}...`);

                // Get chart image
                const imageData = await this.chartToImage(containerId);

                // Chart header
                markdown += `## ${i + 1}. ${chart.source} - ${this.formatDate(chart.date)}\n\n`;

                // Metadata table
                markdown += `| Property | Value |\n`;
                markdown += `|----------|-------|\n`;
                markdown += `| Source | ${chart.source} |\n`;
                markdown += `| Date | ${this.formatDate(chart.date)} |\n`;
                markdown += `| Timezone | ${chart.timezone} |\n`;
                markdown += `| Frequency | ${chart.frequency} |\n`;
                markdown += `| Bars | ${chart.maxBars === 999 ? 'Full day' : chart.maxBars} |\n`;
                markdown += `| Gap Direction | ${chart.gapDirection} |\n`;
                markdown += `| Gap Size | ${chart.gapSizeClass} |\n`;

                if (chart.openAbovePrevHigh) {
                    markdown += `| Open Above Prev High | Yes |\n`;
                }
                if (chart.closeBelowPrevLow) {
                    markdown += `| Close Below Prev Low | Yes |\n`;
                }

                markdown += `\n`;

                // Embed image if available
                if (imageData) {
                    markdown += `![Chart ${i + 1}](${imageData})\n\n`;
                } else {
                    markdown += `*Chart image not available*\n\n`;
                }

                // Bar directions (first 20)
                if (chart.barDirections && chart.barDirections.length > 0) {
                    const dirs = chart.barDirections.slice(0, 20).map((d, idx) => `${idx + 1}:${d}`).join(', ');
                    markdown += `**Bar Directions (1-20):** ${dirs}\n\n`;
                }

                markdown += `---\n\n`;
            }

            // Download the file
            this.downloadFile(markdown, 'chart-export.md', 'text/markdown');
            showProgress('');

        } catch (error) {
            console.error('Export error:', error);
            alert('Error exporting to Markdown: ' + error.message);
            showProgress('');
        }
    },

    // Generate PowerPoint export
    async exportToPowerPoint() {
        if (this.currentCharts.length === 0) {
            alert('No charts to export. Apply filters first.');
            return;
        }

        // Check if PptxGenJS is loaded
        if (typeof PptxGenJS === 'undefined') {
            alert('PowerPoint library not loaded. Please refresh the page and try again.');
            return;
        }

        const showProgress = this.showExportProgress('Generating PowerPoint...');

        try {
            const pptx = new PptxGenJS();

            // Set presentation properties
            pptx.author = 'Chart Viewer';
            pptx.title = 'Chart Export';
            pptx.subject = 'Trading Charts Export';

            // Title slide
            let slide = pptx.addSlide();
            slide.addText('Chart Export', {
                x: 0.5,
                y: 2,
                w: 9,
                h: 1.5,
                fontSize: 44,
                bold: true,
                color: '2c3e50',
                align: 'center'
            });
            slide.addText(`${this.currentCharts.length} Charts | Generated: ${new Date().toLocaleDateString()}`, {
                x: 0.5,
                y: 3.5,
                w: 9,
                h: 0.5,
                fontSize: 18,
                color: '7f8c8d',
                align: 'center'
            });

            // Process each chart
            for (let i = 0; i < this.currentCharts.length; i++) {
                const chart = this.currentCharts[i];
                const containerId = `chart-${i}`;

                showProgress(`Processing chart ${i + 1} of ${this.currentCharts.length}...`);

                // Get chart image
                const imageData = await this.chartToImage(containerId);

                // Create slide
                slide = pptx.addSlide();

                // Title
                slide.addText(`${chart.source} - ${this.formatDate(chart.date)}`, {
                    x: 0.3,
                    y: 0.2,
                    w: 9.4,
                    h: 0.5,
                    fontSize: 24,
                    bold: true,
                    color: '2c3e50'
                });

                // Metadata badges
                const tz = chart.timezone === 'America/New_York' ? 'NY' : 'LON';
                const barsText = chart.maxBars === 999 ? 'Full day' : `${chart.maxBars} bars`;
                const metaText = `${chart.frequency} | ${barsText} | ${chart.gapDirection} | ${chart.gapSizeClass} | ${tz}`;

                slide.addText(metaText, {
                    x: 0.3,
                    y: 0.7,
                    w: 9.4,
                    h: 0.3,
                    fontSize: 12,
                    color: '7f8c8d'
                });

                // Add chart image
                if (imageData) {
                    slide.addImage({
                        data: imageData,
                        x: 0.3,
                        y: 1.1,
                        w: 9.4,
                        h: 4.2
                    });
                } else {
                    slide.addText('Chart image not available', {
                        x: 0.3,
                        y: 2.5,
                        w: 9.4,
                        h: 1,
                        fontSize: 18,
                        color: 'e74c3c',
                        align: 'center'
                    });
                }

                // Slide number
                slide.addText(`${i + 1} / ${this.currentCharts.length}`, {
                    x: 8.5,
                    y: 5.3,
                    w: 1,
                    h: 0.2,
                    fontSize: 10,
                    color: '95a5a6',
                    align: 'right'
                });
            }

            // Save the file
            showProgress('Saving PowerPoint...');
            await pptx.writeFile({ fileName: 'chart-export.pptx' });
            showProgress('');

        } catch (error) {
            console.error('Export error:', error);
            alert('Error exporting to PowerPoint: ' + error.message);
            showProgress('');
        }
    },

    // Show export progress
    showExportProgress(message) {
        let progressEl = document.getElementById('export-progress');
        if (!progressEl) {
            progressEl = document.createElement('div');
            progressEl.id = 'export-progress';
            progressEl.className = 'export-progress';
            document.body.appendChild(progressEl);
        }

        if (message) {
            progressEl.textContent = message;
            progressEl.style.display = 'block';
        } else {
            progressEl.style.display = 'none';
        }

        return (newMessage) => {
            if (newMessage) {
                progressEl.textContent = newMessage;
                progressEl.style.display = 'block';
            } else {
                progressEl.style.display = 'none';
            }
        };
    },

    // Download a file
    downloadFile(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }
};
