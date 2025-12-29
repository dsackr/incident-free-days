document.addEventListener("DOMContentLoaded", function () {
    const buttons = document.querySelectorAll(".tab-button");
    const contents = document.querySelectorAll(".tab-content");

    buttons.forEach((btn) => {
        btn.addEventListener("click", () => {
            const target = btn.getAttribute("data-tab");

            buttons.forEach((b) => b.classList.remove("active"));
            contents.forEach((c) => c.classList.remove("active"));

            btn.classList.add("active");
            document.getElementById(target).classList.add("active");
        });
    });

    const incidentDataEl = document.getElementById("incident-data");
    const otherDataEl = document.getElementById("other-data");
    let incidentsByDate = {};
    let otherEventsByDate = {};

    if (incidentDataEl) {
        try {
            incidentsByDate = JSON.parse(incidentDataEl.textContent || "{}");
        } catch (err) {
            incidentsByDate = {};
        }
    }

    if (otherDataEl) {
        try {
            otherEventsByDate = JSON.parse(otherDataEl.textContent || "{}");
        } catch (err) {
            otherEventsByDate = {};
        }
    }

    const eventsByKind = {
        incidents: incidentsByDate,
        others: otherEventsByDate,
    };

    const readJsonFromScript = (elementId) => {
        const el = document.getElementById(elementId);
        if (!el) return null;

        try {
            return JSON.parse(el.textContent || "{}");
        } catch (err) {
            return null;
        }
    };

    const productPillarMap = readJsonFromScript("product-pillar-map") || {};
    const productsByPillar = readJsonFromScript("products-by-pillar") || {};
    const allProducts = productsByPillar.__all__ || [];

    const modal = document.getElementById("incident-modal");
    const modalDateEl = document.getElementById("incident-modal-date");
    const modalBody = document.getElementById("incident-modal-body");
    const modalClose = document.getElementById("incident-modal-close");
    const modalBackdrop = document.getElementById("incident-modal-backdrop");

    const formatDateTime = (value) => {
        if (!value) return null;

        const parsed = new Date(value);
        if (Number.isNaN(parsed.getTime())) return value;

        const formatter = new Intl.DateTimeFormat("en-US", {
            timeZone: "America/New_York",
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            minute: "2-digit",
            hour12: false,
        });

        const parts = formatter.formatToParts(parsed).reduce((acc, part) => {
            acc[part.type] = part.value;
            return acc;
        }, {});

        if (parts.month && parts.day && parts.year && parts.hour && parts.minute) {
            return `${parts.month}/${parts.day}/${parts.year} ${parts.hour}:${parts.minute}`;
        }

        return formatter.format(parsed);
    };

    const formatDuration = (seconds) => {
        if (seconds === null || seconds === undefined) return null;

        const totalSeconds = Number.parseInt(seconds, 10);
        if (Number.isNaN(totalSeconds) || totalSeconds < 0) return null;

        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor((totalSeconds % 3600) / 60);

        const parts = [];
        if (days) parts.push(`${days}d`);
        if (hours) parts.push(`${hours}h`);
        parts.push(`${minutes}m`);

        return parts.join(" ");
    };

    const renderModal = (kind, dateStr, incidents) => {
        modalDateEl.textContent = "";

        const modalTitle = document.createElement("span");
        modalTitle.textContent = kind === "incidents" ? "Incidents" : "Events";

        const modalDate = document.createElement("span");
        modalDate.classList.add("modal-date");
        modalDate.textContent = `on ${dateStr}`;

        modalDateEl.append(modalTitle, modalDate);

        if (!incidents || incidents.length === 0) {
            modalBody.innerHTML = `<p>No records for ${dateStr}.</p>`;
        } else {
            modalBody.innerHTML = "";
            incidents.forEach((inc) => {
                const row = document.createElement("div");
                row.className = "incident-row";
                const incidentTitle = () => {
                    const label = inc.inc_number || "Incident";
                    if (!inc.inc_number) return label;

                    const incidentId = inc.inc_number.replace(/^INC-/i, "");
                    const idForUrl = incidentId || inc.inc_number;
                    const url = `https://app.incident.io/myfrontline/incidents/${encodeURIComponent(
                        idForUrl
                    )}`;
                    return `<a class="incident-link" href="${url}" target="_blank" rel="noopener noreferrer">${label}</a>`;
                };
                const extraMeta = [];
                const reportedFormatted = formatDateTime(inc.reported_at);
                const closedFormatted = formatDateTime(inc.closed_at);
                const clientDurationLabel =
                    inc.client_impact_duration_label || formatDuration(inc.client_impact_duration_seconds);

                if (reportedFormatted) {
                    extraMeta.push(`Reported: ${reportedFormatted}`);
                }
                if (closedFormatted) {
                    extraMeta.push(`Closed: ${closedFormatted}`);
                }
                if (clientDurationLabel) {
                    extraMeta.push(`Client impact: ${clientDurationLabel}`);
                }
                const extraMetaText = extraMeta.length ? ` · ${extraMeta.join(" · ")}` : "";
                const eventType = inc.event_type ? ` · Type: ${inc.event_type}` : "";
                const metaParts = [];
                if (kind === "incidents") {
                    metaParts.push(`Severity: ${inc.severity || "N/A"}`);
                }
                metaParts.push(`Pillar: ${inc.pillar || "N/A"}`);
                metaParts.push(`Product: ${inc.product || "N/A"}`);
                const metaText = metaParts.join(" · ");
                row.innerHTML = `
                    <p class="incident-title">${incidentTitle()}</p>
                    <p class="incident-meta">${metaText}${eventType}${extraMetaText}</p>
                `;
                modalBody.appendChild(row);
            });
        }

        modal.classList.add("open");
        modal.setAttribute("aria-hidden", "false");
    };

    const closeModal = () => {
        modal.classList.remove("open");
        modal.setAttribute("aria-hidden", "true");
    };

    document.querySelectorAll("td[data-date]").forEach((cell) => {
        cell.addEventListener("click", () => {
            const dateStr = cell.getAttribute("data-date");
            const kind = cell.getAttribute("data-calendar-kind") || "incidents";
            if (!dateStr) return;
            const source = eventsByKind[kind] || {};
            const incidents = source[dateStr] || [];
            renderModal(kind, dateStr, incidents);
        });
    });

    modalClose?.addEventListener("click", closeModal);
    modalBackdrop?.addEventListener("click", closeModal);
    document.addEventListener("keydown", (evt) => {
        if (evt.key === "Escape" && modal.classList.contains("open")) {
            closeModal();
        }
    });

    const exportButtons = document.querySelectorAll(".export-calendar");

    const setupCheckboxDropdown = ({ dropdownId, toggleId, menuId, selectionLabelId, inputName }) => {
        const dropdown = document.getElementById(dropdownId);
        const toggle = document.getElementById(toggleId);
        const menu = document.getElementById(menuId);
        const selectionLabel = document.getElementById(selectionLabelId);

        const updateLabel = () => {
            if (!dropdown || !selectionLabel) return;
            const checked = dropdown.querySelectorAll(`input[name='${inputName}']:checked`);
            if (checked.length === 0) {
                selectionLabel.textContent = "All";
                return;
            }

            const values = Array.from(checked)
                .map((input) => input.value)
                .filter(Boolean);
            selectionLabel.textContent = values.join(", ") || "All";
        };

        if (toggle && dropdown) {
            toggle.addEventListener("click", (evt) => {
                evt.preventDefault();
                const isOpen = dropdown.classList.toggle("open");
                toggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
            });

            document.addEventListener("click", (evt) => {
                if (!dropdown.contains(evt.target)) {
                    dropdown.classList.remove("open");
                    toggle.setAttribute("aria-expanded", "false");
                }
            });

            menu?.addEventListener("change", (evt) => {
                if (evt.target && evt.target.matches(`input[name='${inputName}']`)) {
                    updateLabel();
                }
            });

            updateLabel();
        }
    };

    setupCheckboxDropdown({
        dropdownId: "event-type-dropdown",
        toggleId: "event-type-dropdown-toggle",
        menuId: "event-type-dropdown-menu",
        selectionLabelId: "event-type-selection-label",
        inputName: "event_type",
    });

    const setupAutoSubmitFilters = () => {
        const forms = document.querySelectorAll(".control-form");

        forms.forEach((form) => {
            const triggerSubmit = () => {
                if (typeof form.requestSubmit === "function") {
                    form.requestSubmit();
                } else {
                    form.submit();
                }
            };

            const inputs = form.querySelectorAll("select, input[type='checkbox']");
            inputs.forEach((input) => {
                if (input.dataset.manualSubmit === "true") return;
                input.addEventListener("change", triggerSubmit);
            });
        });
    };

    const setupCollapsibles = () => {
        document.querySelectorAll("[data-collapsible-toggle]").forEach((btn) => {
            const panelId = btn.getAttribute("aria-controls") || `${btn.dataset.collapsibleToggle}-panel`;
            const panel = document.getElementById(panelId);
            if (!panel) return;

            const togglePanel = () => {
                const isHidden = panel.hasAttribute("hidden");
                if (isHidden) {
                    panel.removeAttribute("hidden");
                    btn.setAttribute("aria-expanded", "true");
                } else {
                    panel.setAttribute("hidden", "hidden");
                    btn.setAttribute("aria-expanded", "false");
                }
            };

            btn.addEventListener("click", togglePanel);
        });
    };

    const updateProductSummary = (container, products, pillars) => {
        if (!container) return;
        container.innerHTML = "";
        const addChip = (label, muted = false) => {
            const chip = document.createElement("span");
            chip.className = muted ? "filter-chip muted" : "filter-chip";
            chip.textContent = label;
            container.appendChild(chip);
        };

        if (products.size > 0) {
            Array.from(products)
                .sort()
                .forEach((prod) => addChip(prod));
            return;
        }

        if (pillars.size > 0) {
            Array.from(pillars)
                .sort()
                .forEach((pill) => addChip(pill));
            return;
        }

        addChip("All products", true);
    };

    const applyActivePillar = (buttons, panels, targetPillar) => {
        buttons.forEach((btn) => {
            const isActive = btn.dataset.pillarToggle === targetPillar;
            btn.classList.toggle("active", isActive);
            btn.setAttribute("aria-selected", isActive ? "true" : "false");
        });

        panels.forEach((panel) => {
            const isMatch = panel.dataset.pillarProducts === targetPillar;
            if (isMatch) {
                panel.removeAttribute("hidden");
            } else {
                panel.setAttribute("hidden", "hidden");
            }
        });
    };

    const setupPillarProductFilters = (formId) => {
        const form = document.getElementById(formId);
        if (!form) return;

        const productCheckboxes = form.querySelectorAll("[data-product-option]");
        if (!productCheckboxes.length) return;

        const pillars = form.querySelectorAll("[data-pillar-toggle]");
        const panels = form.querySelectorAll("[data-pillar-products]");
        const hiddenInputs = form.querySelector("[id$='product-filter-hidden-inputs']");
        const summary = form.querySelector("[data-product-summary]");
        const saveButton = form.querySelector("[data-save-products]");
        const clearAllButton = form.querySelector("[data-clear-all-products]");

        const getSelectedProducts = () =>
            new Set(
                Array.from(productCheckboxes)
                    .filter((input) => input.checked)
                    .map((input) => input.value)
            );

        const derivePillarsFromProducts = (products) => {
            const derived = new Set();
            products.forEach((prod) => {
                const pillar = productPillarMap[prod];
                if (pillar) derived.add(pillar);
            });
            return derived;
        };

        const updateHiddenInputs = (products) => {
            if (!hiddenInputs) return;
            hiddenInputs.innerHTML = "";
            const pillarsForProducts = derivePillarsFromProducts(products);

            pillarsForProducts.forEach((pillar) => {
                const input = document.createElement("input");
                input.type = "hidden";
                input.name = "pillar";
                input.value = pillar;
                hiddenInputs.appendChild(input);
            });

            products.forEach((product) => {
                const input = document.createElement("input");
                input.type = "hidden";
                input.name = "product";
                input.value = product;
                hiddenInputs.appendChild(input);
            });

            updateProductSummary(summary, products, pillarsForProducts);
            return pillarsForProducts;
        };

        const submitForm = () => {
            if (typeof form.requestSubmit === "function") {
                form.requestSubmit();
            } else {
                form.submit();
            }
        };

        const productPanel = form.querySelector("[id$='product-filter-panel']");
        const collapsePanel = () => {
            if (!productPanel) return;
            const toggle = form.querySelector(`[aria-controls='${productPanel.id}']`);
            productPanel.setAttribute("hidden", "hidden");
            toggle?.setAttribute("aria-expanded", "false");
        };

        const activateDefaultPillar = () => {
            const selected = Array.from(productCheckboxes).find((input) => input.checked);
            const targetPillar = selected?.dataset.pillar || pillars[0]?.dataset.pillarToggle;
            if (targetPillar) {
                applyActivePillar(pillars, panels, targetPillar);
            }
        };

        pillars.forEach((btn) => {
            btn.addEventListener("click", () => {
                applyActivePillar(pillars, panels, btn.dataset.pillarToggle);
            });
        });

        form.querySelectorAll("[data-select-all]").forEach((btn) => {
            btn.addEventListener("click", () => {
                const target = btn.getAttribute("data-select-all");
                productCheckboxes.forEach((input) => {
                    if (input.dataset.pillar === target) {
                        input.checked = true;
                    }
                });
            });
        });

        form.querySelectorAll("[data-clear-pillar]").forEach((btn) => {
            btn.addEventListener("click", () => {
                const target = btn.getAttribute("data-clear-pillar");
                productCheckboxes.forEach((input) => {
                    if (input.dataset.pillar === target) {
                        input.checked = false;
                    }
                });
            });
        });

        saveButton?.addEventListener("click", () => {
            const selectedProducts = getSelectedProducts();
            updateHiddenInputs(selectedProducts);
            collapsePanel();
            submitForm();
        });

        clearAllButton?.addEventListener("click", () => {
            productCheckboxes.forEach((input) => {
                input.checked = false;
            });
            updateHiddenInputs(new Set());
            collapsePanel();
            submitForm();
        });

        activateDefaultPillar();
        updateHiddenInputs(getSelectedProducts());
    };

    setupCollapsibles();
    setupAutoSubmitFilters();
    setupPillarProductFilters("incident-filter-form");
    setupPillarProductFilters("incident-table-filter-form");
    setupPillarProductFilters("other-filter-form");

    const incidentTable = document.getElementById("incident-table");

    if (incidentTable) {
        const tableBody = incidentTable.querySelector("tbody");
        const headerCells = incidentTable.querySelectorAll("th[data-sort-key]");
        const sortSelect = document.getElementById("incident-table-sort");
        const sortDirection = document.getElementById("incident-table-direction");
        const filterColumnSelect = document.getElementById("incident-table-filter-column");
        const filterTextInput = document.getElementById("incident-table-filter-text");

        const allRows = Array.from(tableBody.querySelectorAll("tr"));

        const columnTypes = {
            reported_at: "date",
            closed_at: "date",
            duration_seconds: "number",
        };

        const searchableKeys = [
            "inc_number",
            "product",
            "pillar",
            "severity",
            "rca_classification",
            "event_type",
        ];

        const normalizeValue = (key, value) => {
            if (!value) return "";
            if (columnTypes[key] === "date") {
                const parsed = Date.parse(value);
                return Number.isNaN(parsed) ? 0 : parsed;
            }
            if (columnTypes[key] === "number") {
                const parsed = Number.parseInt(value, 10);
                return Number.isNaN(parsed) ? 0 : parsed;
            }

            return value.toString().toLowerCase();
        };

        const setHeaderSortState = (key, direction) => {
            headerCells.forEach((cell) => {
                const cellKey = cell.getAttribute("data-sort-key");
                if (cellKey === key) {
                    cell.setAttribute(
                        "aria-sort",
                        direction === "asc" ? "ascending" : "descending"
                    );
                } else {
                    cell.setAttribute("aria-sort", "none");
                }
            });
        };

        const applySortAndFilter = () => {
            const sortKey = sortSelect?.value || "reported_at";
            const direction = sortDirection?.value === "asc" ? "asc" : "desc";
            const filterKey = filterColumnSelect?.value || "all";
            const filterText = (filterTextInput?.value || "").trim().toLowerCase();

            const filtered = allRows.filter((row) => {
                if (!filterText) return true;

                if (filterKey === "all") {
                    return searchableKeys.some((key) => {
                        const value = row.dataset[key] || "";
                        return value.toString().toLowerCase().includes(filterText);
                    });
                }

                const value = row.dataset[filterKey] || "";
                return value.toString().toLowerCase().includes(filterText);
            });

            filtered.sort((a, b) => {
                const aVal = normalizeValue(sortKey, a.dataset[sortKey] || "");
                const bVal = normalizeValue(sortKey, b.dataset[sortKey] || "");

                if (aVal === bVal) return 0;
                const comparison = aVal < bVal ? -1 : 1;
                return direction === "asc" ? comparison : -comparison;
            });

            tableBody.innerHTML = "";
            filtered.forEach((row) => tableBody.appendChild(row));

            setHeaderSortState(sortKey, direction);
        };

        const toggleSortFromHeader = (headerCell) => {
            const key = headerCell.getAttribute("data-sort-key");
            if (!key) return;

            const currentDirection = headerCell.getAttribute("aria-sort");
            const nextDirection = currentDirection === "ascending" ? "desc" : "asc";

            if (sortSelect) sortSelect.value = key;
            if (sortDirection) sortDirection.value = nextDirection;

            applySortAndFilter();
        };

        headerCells.forEach((cell) => {
            cell.addEventListener("click", () => toggleSortFromHeader(cell));
        });

        sortSelect?.addEventListener("change", applySortAndFilter);
        sortDirection?.addEventListener("change", applySortAndFilter);
        filterColumnSelect?.addEventListener("change", applySortAndFilter);
        filterTextInput?.addEventListener("input", applySortAndFilter);

        applySortAndFilter();
    }

    const buildStyleString = (computed) =>
        Array.from(computed)
            .map((prop) => `${prop}:${computed.getPropertyValue(prop)};`)
            .join("");

    const cloneNodeWithInlineStyles = (node) => {
        const clone = node.cloneNode(false);

        if (node.nodeType === Node.ELEMENT_NODE) {
            const computed = window.getComputedStyle(node);
            clone.setAttribute("style", buildStyleString(computed));
        }

        node.childNodes.forEach((child) => {
            clone.appendChild(cloneNodeWithInlineStyles(child));
        });

        return clone;
    };

    const renderWithInlineSvg = async (exportTarget) => {
        if (!exportTarget) {
            console.error("Inline SVG export failed: no export target provided.");
            return null;
        }

        let svgUrl = null;

        const cloned = cloneNodeWithInlineStyles(exportTarget);
        const width = exportTarget.offsetWidth;
        const height = exportTarget.offsetHeight;
        const serializer = new XMLSerializer();

        const serialized = serializer.serializeToString(cloned);
        const svg = `
                <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}">
                    <foreignObject width="100%" height="100%">
                        <div xmlns="http://www.w3.org/1999/xhtml">${serialized}</div>
                    </foreignObject>
                </svg>
            `;

        const svgBlob = new Blob([svg], { type: "image/svg+xml;charset=utf-8" });
        svgUrl = URL.createObjectURL(svgBlob);

        const image = new Image();

        await new Promise((resolve, reject) => {
            image.onload = () => resolve();
            image.onerror = () => reject(new Error("Could not load the exported SVG"));
            image.src = svgUrl;
        });

        return {
            image,
            cleanup: () => {
                if (svgUrl) {
                    URL.revokeObjectURL(svgUrl);
                }
            },
        };
    };

    const renderWithHtml2Canvas = async (exportTarget) => {
        if (!exportTarget) {
            console.error("html2canvas export failed: no export target provided.");
            return null;
        }

        if (typeof html2canvas !== "function") {
            console.error("html2canvas export failed: library not loaded or blocked by CSP/SRI.");
            return null;
        }

        try {
            const externalImages = Array.from(exportTarget.querySelectorAll("img")).filter((img) => {
                try {
                    const url = new URL(img.src, window.location.href);
                    return url.origin !== window.location.origin;
                } catch (err) {
                    return false;
                }
            });

            externalImages.forEach((img) => {
                // Encourage safe CORS requests so canvases stay untainted by external assets.
                if (!img.crossOrigin) {
                    img.crossOrigin = "anonymous";
                }
            });

            const snapshot = await html2canvas(exportTarget, {
                scale: 2,
                backgroundColor: "#ffffff",
                useCORS: true,
                onclone: (clonedDoc) => {
                    externalImages.forEach((img) => {
                        const selector = img.getAttribute("data-export-selector");
                        const clonedImg = selector
                            ? clonedDoc.querySelector(selector)
                            : clonedDoc.querySelector(`img[src='${img.src}']`);
                        if (clonedImg && !clonedImg.crossOrigin) {
                            clonedImg.crossOrigin = "anonymous";
                        }
                    });
                },
            });

            return { canvas: snapshot };
        } catch (err) {
            console.error("html2canvas capture failed; will try inline SVG fallback.", err);
            return null;
        }
    };

    const exportCalendar = async (exportButton) => {
        const targetId = exportButton?.dataset?.target;
        const exportTarget = targetId ? document.getElementById(targetId) : null;
        if (!exportButton) {
            console.error("Export failed: export button reference is missing.");
            alert("Sorry, the calendar could not be exported. Please try again.");
            return;
        }
        if (!exportTarget) {
            console.error("Export failed: no export target found for", targetId);
            alert("Sorry, the calendar could not be exported. Please try again.");
            return;
        }

        let cleanup = null;

        exportButton.disabled = true;
        exportButton.textContent = "Exporting...";

        try {
            const html2canvasResult = await renderWithHtml2Canvas(exportTarget);
            let source = null;

            if (html2canvasResult?.canvas) {
                source = {
                    width: html2canvasResult.canvas.width,
                    height: html2canvasResult.canvas.height,
                    draw: (ctx, dx, dy, drawWidth, drawHeight) =>
                        ctx.drawImage(html2canvasResult.canvas, dx, dy, drawWidth, drawHeight),
                };
            } else {
                const svgResult = await renderWithInlineSvg(exportTarget);
                cleanup = svgResult?.cleanup;

                if (svgResult?.image) {
                    source = {
                        width: svgResult.image.width,
                        height: svgResult.image.height,
                        draw: (ctx, dx, dy, drawWidth, drawHeight) =>
                            ctx.drawImage(svgResult.image, dx, dy, drawWidth, drawHeight),
                    };
                }
            }

            if (!source) {
                throw new Error("Unable to render calendar for export");
            }

            const canvas = document.createElement("canvas");
            canvas.width = 1600;
            canvas.height = 1200;

            const ctx = canvas.getContext("2d");
            if (!ctx) throw new Error("Canvas context unavailable");

            ctx.fillStyle = "#ffffff";
            ctx.fillRect(0, 0, canvas.width, canvas.height);

            const ratio = Math.min(canvas.width / source.width, canvas.height / source.height);
            const drawWidth = source.width * ratio;
            const drawHeight = source.height * ratio;
            const dx = (canvas.width - drawWidth) / 2;
            const dy = (canvas.height - drawHeight) / 2;
            source.draw(ctx, dx, dy, drawWidth, drawHeight);

            const viewMode = exportButton.dataset.viewMode || "yearly";
            const year = exportButton.dataset.year || "calendar";
            const monthName = exportButton.dataset.monthName || "";
            const sanitizedMonth = monthName ? `-${monthName.replace(/\s+/g, "-")}` : "";
            const filename = `incident-calendar-${viewMode}-${year}${sanitizedMonth}.png`;

            canvas.toBlob(
                (blob) => {
                    if (!blob) {
                        console.error("Calendar export failed: browser could not generate PNG blob (possible CSP/canvas restriction).");
                        alert("Sorry, the calendar could not be exported. Please try again.");
                        return;
                    }
                    const link = document.createElement("a");
                    link.href = URL.createObjectURL(blob);
                    link.download = filename;
                    link.click();
                    URL.revokeObjectURL(link.href);
                },
                "image/png"
            );
        } catch (err) {
            console.error("Calendar export failed", err);
            alert("Sorry, the calendar could not be exported. Please try again.");
        } finally {
            cleanup?.();
            exportButton.disabled = false;
            exportButton.textContent = "Export as Image";
        }
    };

    exportButtons.forEach((btn) => {
        btn.addEventListener("click", () => exportCalendar(btn));
    });

    // Sync configuration helpers
    const apiTokenInput = document.getElementById("sync-api-token");
    const baseUrlInput = document.getElementById("sync-base-url");
    const cadenceSelect = document.getElementById("sync-cadence");
    const startDateInput = document.getElementById("sync-start-date");
    const endDateInput = document.getElementById("sync-end-date");
    const saveSettingsButton = document.getElementById("save-sync-settings");
    const dryRunButton = document.getElementById("sync-dry-run");
    const importButton = document.getElementById("sync-import");
    const wipeButton = document.getElementById("wipe-data");
    const resultsEl = document.getElementById("sync-results");
    const previewBody = document.getElementById("mapping-preview-body");
    const statusPillContainer = document.getElementById("sync-status-pill");
    const syncConfigData = readJsonFromScript("sync-config-data") || {};

    const setStatusPill = (status, text) => {
        if (!statusPillContainer) return;
        statusPillContainer.innerHTML = "";
        if (!status || !text) return;

        const pill = document.createElement("span");
        pill.className = `status-pill ${status}`;
        pill.textContent = text;
        statusPillContainer.appendChild(pill);
    };

    const renderSamples = (samples) => {
        if (!previewBody) return;
        previewBody.innerHTML = "";

        if (!samples || samples.length === 0) {
            const row = document.createElement("tr");
            row.className = "empty-row";
            const cell = document.createElement("td");
            cell.colSpan = 8;
            cell.textContent = "Run a dry run to see mapping results.";
            row.appendChild(cell);
            previewBody.appendChild(row);
            return;
        }

        samples.forEach((sample) => {
            const normalized = sample.normalized || {};
            const row = document.createElement("tr");
            const clientDurationDisplay =
                normalized.client_impact_duration_label || formatDuration(normalized.client_impact_duration_seconds);
            const columns = [
                normalized.inc_number || sample?.source?.id || "",
                normalized.product || "",
                normalized.pillar || "",
                normalized.severity || "",
                normalized.reported_at || "",
                normalized.closed_at || "",
                clientDurationDisplay || "",
                normalized.event_type || "",
            ];

            columns.forEach((value) => {
                const cell = document.createElement("td");
                cell.textContent = value === null || value === undefined ? "" : value;
                row.appendChild(cell);
            });

            previewBody.appendChild(row);
        });
    };

    const renderResults = (result) => {
        if (!resultsEl) return;
        if (!result) {
            resultsEl.innerHTML = "";
            return;
        }

        if (result.error) {
            resultsEl.innerHTML = `<div class="notice error">${result.error}</div>`;
            setStatusPill("error", "Sync failed");
            return;
        }

        const summary = document.createElement("div");
        summary.className = "notice success";
        summary.innerHTML = `Fetched ${result.fetched} • Added incidents: ${result.added_incidents} • Added other events: ${result.added_other_events} • Updated: ${result.updated_events || 0} ${result.dry_run ? "(dry run)" : ""}`;
        resultsEl.innerHTML = "";
        resultsEl.appendChild(summary);

        setStatusPill("success", result.dry_run ? "Dry run complete" : "Import complete");
    };

    const gatherSyncPayload = (dryRun) => {
        const tokenValue = apiTokenInput?.value?.trim();
        return {
            dry_run: !!dryRun,
            token: tokenValue || undefined,
            base_url: baseUrlInput?.value?.trim() || undefined,
            start_date: startDateInput?.value || undefined,
            end_date: endDateInput?.value || undefined,
            include_samples: true,
            persist_settings: true,
            cadence: cadenceSelect?.value || "daily",
        };
    };

    const handleSyncRequest = async (dryRun) => {
        if (!dryRun && !confirm("Import incidents into the calendar?")) {
            return;
        }

        renderResults(null);
        setStatusPill("info", dryRun ? "Running dry run" : "Importing incidents");

        try {
            const response = await fetch("/sync/incidents", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(gatherSyncPayload(dryRun)),
            });

            const data = await response.json();
            if (!response.ok) {
                renderResults({ error: data?.error || "Sync failed" });
                return;
            }

            renderResults(data);
            renderSamples(data?.samples || []);
        } catch (err) {
            console.error("Sync failed", err);
            renderResults({ error: "Unable to reach the sync endpoint." });
        }
    };

    const handleSaveSettings = async () => {
        setStatusPill("info", "Saving settings");
        try {
            const response = await fetch("/sync/config", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    token: apiTokenInput?.value?.trim() || undefined,
                    base_url: baseUrlInput?.value?.trim() || undefined,
                    cadence: cadenceSelect?.value || "daily",
                    start_date: startDateInput?.value || undefined,
                    end_date: endDateInput?.value || undefined,
                }),
            });

            const data = await response.json();
            if (!response.ok) {
                setStatusPill("error", "Save failed");
                resultsEl.innerHTML = `<div class="notice error">${data?.error || "Could not save settings."}</div>`;
                return;
            }

            setStatusPill("success", "Settings saved");
        } catch (err) {
            console.error("Save failed", err);
            setStatusPill("error", "Save failed");
        }
    };

    const handleWipeData = async () => {
        if (!confirm("Remove all local incident data? This cannot be undone.")) {
            return;
        }

        setStatusPill("warning", "Clearing local data");
        try {
            const response = await fetch("/sync/wipe", { method: "POST" });
            if (!response.ok) {
                resultsEl.innerHTML = `<div class="notice error">Failed to delete local data.</div>`;
                setStatusPill("error", "Wipe failed");
                return;
            }

            resultsEl.innerHTML = `<div class="notice success">Local incident data cleared.</div>`;
            setStatusPill("success", "Data cleared");
        } catch (err) {
            console.error("Wipe failed", err);
            resultsEl.innerHTML = `<div class="notice error">Unable to clear data.</div>`;
            setStatusPill("error", "Wipe failed");
        }
    };

    if (syncConfigData?.last_sync_display) {
        setStatusPill("info", `Last sync ${syncConfigData.last_sync_display} ET`);
    } else if (syncConfigData?.last_sync?.timestamp) {
        setStatusPill("info", `Last sync ${syncConfigData.last_sync.timestamp}`);
    }

    const oshaForm = document.getElementById("send-osha-display-form");
    const oshaButton = document.getElementById("send-osha-display-btn");
    const progressCard = document.getElementById("osha-send-progress");
    const progressFill = document.getElementById("osha-send-progress-fill");
    const progressBar = document.getElementById("osha-send-progress-bar");
    const progressLabel = document.getElementById("osha-send-progress-label");
    const progressCount = document.getElementById("osha-send-progress-count");
    const progressStatus = document.getElementById("osha-send-status");

    if (oshaForm && oshaButton && progressCard && progressFill && progressBar) {
        let controller = null;
        const resetProgress = () => {
            progressFill.style.width = "0%";
            progressBar.setAttribute("aria-valuenow", "0");
            progressLabel.textContent = "Preparing to send…";
            progressCount.textContent = "";
            progressStatus.textContent = "";
            progressStatus.removeAttribute("data-state");
        };

        const updateProgress = (chunk, total) => {
            const percentage = total ? Math.min(100, Math.round((chunk / total) * 100)) : 0;
            progressFill.style.width = `${percentage}%`;
            progressBar.setAttribute("aria-valuenow", String(percentage));
            if (total) {
                progressCount.textContent = `${chunk} / ${total} chunks`;
            }
        };

        const setStatus = (message, state = "info") => {
            progressStatus.textContent = message;
            progressStatus.dataset.state = state;
        };

        oshaForm.addEventListener("submit", async (event) => {
            event.preventDefault();
            if (controller) {
                return;
            }

            controller = new AbortController();
            resetProgress();
            progressCard.hidden = false;
            oshaButton.disabled = true;
            oshaButton.textContent = "Sending…";

            try {
                const response = await fetch("/api/osha/send_to_display", {
                    method: "POST",
                    signal: controller.signal,
                });

                if (!response.ok || !response.body) {
                    let errorMessage = `Display request failed (${response.status})`;
                    try {
                        const errorText = await response.text();
                        const parsed = JSON.parse(errorText);
                        errorMessage = parsed.message || errorMessage;
                    } catch (err) {
                        // Ignore parse failures; fall back to default error message.
                    }

                    throw new Error(errorMessage);
                }

                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = "";
                let totalChunks = 0;
                let currentChunk = 0;

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;
                    buffer += decoder.decode(value, { stream: true });
                    const lines = buffer.split("\n");
                    buffer = lines.pop();

                    lines.forEach((line) => {
                        if (!line.trim()) return;
                        let payload;
                        try {
                            payload = JSON.parse(line);
                        } catch (err) {
                            console.error("Unable to parse display progress", err);
                            return;
                        }

                        if (payload.status === "start" || payload.status === "starting") {
                            totalChunks = payload.total_chunks || 0;
                            updateProgress(0, totalChunks);
                            progressLabel.textContent = "Sending to display…";
                        } else if (payload.status === "chunk") {
                            currentChunk = payload.chunk || currentChunk;
                            updateProgress(currentChunk, payload.total_chunks || totalChunks);
                            progressLabel.textContent = "Sending to display…";
                        } else if (payload.status === "error") {
                            setStatus(payload.message || "Unable to send to display", "error");
                        } else if (payload.status === "done") {
                            if (payload.success) {
                                setStatus("Display updated", "success");
                                updateProgress(totalChunks || currentChunk || 1, totalChunks || currentChunk || 1);
                            } else {
                                setStatus(payload.message || "Unable to send to display", "error");
                            }
                        }
                    });
                }
            } catch (err) {
                console.error("Send to display failed", err);
                setStatus("Unable to send to display", "error");
            } finally {
                oshaButton.disabled = false;
                oshaButton.textContent = "Send to E-Paper display";
                controller = null;
            }
        });
    }

    dryRunButton?.addEventListener("click", () => handleSyncRequest(true));
    importButton?.addEventListener("click", () => handleSyncRequest(false));
    saveSettingsButton?.addEventListener("click", handleSaveSettings);
    wipeButton?.addEventListener("click", handleWipeData);
});
