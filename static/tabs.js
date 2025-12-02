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
    const pillarSelect = document.querySelector("select[name='pillar']");
    const productSelect = document.querySelector("select[name='product']");

    const modal = document.getElementById("incident-modal");
    const modalDateEl = document.getElementById("incident-modal-date");
    const modalBody = document.getElementById("incident-modal-body");
    const modalClose = document.getElementById("incident-modal-close");
    const modalBackdrop = document.getElementById("incident-modal-backdrop");

    const renderModal = (kind, dateStr, incidents) => {
        modalDateEl.textContent = `${kind === "incidents" ? "Incidents" : "Events"} on ${dateStr}`;

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

                    const url = `https://app.incident.io/myfrontline/incidents/${encodeURIComponent(
                        inc.inc_number
                    )}`;
                    return `<a class="incident-link" href="${url}" target="_blank" rel="noopener noreferrer">${label}</a>`;
                };
                const extraMeta = [];
                if (inc.reported_at) {
                    extraMeta.push(`Reported: ${inc.reported_at}`);
                }
                if (inc.closed_at) {
                    extraMeta.push(`Closed: ${inc.closed_at}`);
                }
                if (inc.duration_seconds) {
                    extraMeta.push(`Duration: ${inc.duration_seconds} sec`);
                }
                const extraMetaText = extraMeta.length ? ` · ${extraMeta.join(" · ")}` : "";
                const eventType = inc.event_type ? ` · Type: ${inc.event_type}` : "";
                row.innerHTML = `
                    <p class="incident-title">${incidentTitle()}</p>
                    <p class="incident-meta">Severity: ${inc.severity || "N/A"} · Pillar: ${inc.pillar || "N/A"} · Product: ${inc.product || "N/A"}${eventType}${extraMetaText}</p>
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

    const setupCheckboxDropdown = ({
        dropdownId,
        toggleId,
        menuId,
        selectionLabelId,
        inputName,
    }) => {
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
        dropdownId: "severity-dropdown",
        toggleId: "severity-dropdown-toggle",
        menuId: "severity-dropdown-menu",
        selectionLabelId: "severity-selection-label",
        inputName: "severity",
    });

    setupCheckboxDropdown({
        dropdownId: "event-type-dropdown",
        toggleId: "event-type-dropdown-toggle",
        menuId: "event-type-dropdown-menu",
        selectionLabelId: "event-type-selection-label",
        inputName: "event_type",
    });

    const rebuildProductOptions = (allowedProducts) => {
        if (!productSelect) return;

        const currentValue = productSelect.value;
        productSelect.innerHTML = "";

        const allOption = document.createElement("option");
        allOption.value = "";
        allOption.textContent = "All";
        productSelect.appendChild(allOption);

        allowedProducts.forEach((product) => {
            const option = document.createElement("option");
            option.value = product;
            option.textContent = product;
            productSelect.appendChild(option);
        });

        if (currentValue && allowedProducts.includes(currentValue)) {
            productSelect.value = currentValue;
        } else {
            productSelect.value = "";
        }
    };

    const updateProductsForPillar = () => {
        if (!productSelect) return;

        const selectedPillar = pillarSelect?.value || "";
        const allowedProducts =
            (selectedPillar && productsByPillar[selectedPillar]) || allProducts;

        rebuildProductOptions(allowedProducts);

        if (
            productSelect.value &&
            Array.isArray(allowedProducts) &&
            !allowedProducts.includes(productSelect.value)
        ) {
            productSelect.value = "";
        }
    };

    const syncPillarToProduct = () => {
        if (!productSelect || !pillarSelect) return;

        const selectedProduct = productSelect.value;
        const mappedPillar = productPillarMap[selectedProduct];
        if (mappedPillar && pillarSelect.value !== mappedPillar) {
            pillarSelect.value = mappedPillar;
            updateProductsForPillar();
        }
    };

    pillarSelect?.addEventListener("change", updateProductsForPillar);
    productSelect?.addEventListener("change", syncPillarToProduct);

    updateProductsForPillar();
    syncPillarToProduct();

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
        if (!exportTarget) return null;

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
        if (typeof html2canvas !== "function" || !exportTarget) return null;

        const snapshot = await html2canvas(exportTarget, {
            scale: 2,
            backgroundColor: "#ffffff",
            useCORS: true,
        });

        return { canvas: snapshot };
    };

    const exportCalendar = async (exportButton) => {
        const targetId = exportButton?.dataset?.target;
        const exportTarget = targetId ? document.getElementById(targetId) : null;
        if (!exportButton || !exportTarget) return;

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

            canvas.toBlob((blob) => {
                if (!blob) return;
                const link = document.createElement("a");
                link.href = URL.createObjectURL(blob);
                link.download = filename;
                link.click();
                URL.revokeObjectURL(link.href);
            });
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
});
