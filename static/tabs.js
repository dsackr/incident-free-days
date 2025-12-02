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
    let incidentsByDate = {};

    if (incidentDataEl) {
        try {
            incidentsByDate = JSON.parse(incidentDataEl.textContent || "{}");
        } catch (err) {
            incidentsByDate = {};
        }
    }

    const modal = document.getElementById("incident-modal");
    const modalDateEl = document.getElementById("incident-modal-date");
    const modalBody = document.getElementById("incident-modal-body");
    const modalClose = document.getElementById("incident-modal-close");
    const modalBackdrop = document.getElementById("incident-modal-backdrop");

    const renderModal = (dateStr, incidents) => {
        modalDateEl.textContent = `Incidents on ${dateStr}`;

        if (!incidents || incidents.length === 0) {
            modalBody.innerHTML = `<p>No incidents recorded for ${dateStr}.</p>`;
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
                row.innerHTML = `
                    <p class="incident-title">${incidentTitle()}</p>
                    <p class="incident-meta">Severity: ${inc.severity || "N/A"} · Pillar: ${inc.pillar || "N/A"} · Product: ${inc.product || "N/A"}</p>
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
            if (!dateStr) return;
            const incidents = incidentsByDate[dateStr] || [];
            renderModal(dateStr, incidents);
        });
    });

    modalClose?.addEventListener("click", closeModal);
    modalBackdrop?.addEventListener("click", closeModal);
    document.addEventListener("keydown", (evt) => {
        if (evt.key === "Escape" && modal.classList.contains("open")) {
            closeModal();
        }
    });

    const exportButton = document.getElementById("export-calendar");
    const exportTarget = document.getElementById("calendar-export-target");
    const severityDropdown = document.getElementById("severity-dropdown");
    const severityToggle = document.getElementById("severity-dropdown-toggle");
    const severityMenu = document.getElementById("severity-dropdown-menu");
    const severitySelectionLabel = document.getElementById("severity-selection-label");

    const updateSeverityLabel = () => {
        if (!severityDropdown || !severitySelectionLabel) return;
        const checked = severityDropdown.querySelectorAll("input[name='severity']:checked");
        if (checked.length === 0) {
            severitySelectionLabel.textContent = "All";
            return;
        }

        const values = Array.from(checked)
            .map((input) => input.value)
            .filter(Boolean);
        severitySelectionLabel.textContent = values.join(", ") || "All";
    };

    if (severityToggle && severityDropdown) {
        severityToggle.addEventListener("click", (evt) => {
            evt.preventDefault();
            const isOpen = severityDropdown.classList.toggle("open");
            severityToggle.setAttribute("aria-expanded", isOpen ? "true" : "false");
        });

        document.addEventListener("click", (evt) => {
            if (!severityDropdown.contains(evt.target)) {
                severityDropdown.classList.remove("open");
                severityToggle.setAttribute("aria-expanded", "false");
            }
        });

        severityMenu?.addEventListener("change", (evt) => {
            if (evt.target && evt.target.matches("input[name='severity']")) {
                updateSeverityLabel();
            }
        });

        updateSeverityLabel();
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

    const exportCalendar = async () => {
        if (!exportButton || !exportTarget) return;

        let svgUrl = null;

        exportButton.disabled = true;
        exportButton.textContent = "Exporting...";

        try {
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

            const canvas = document.createElement("canvas");
            canvas.width = 1600;
            canvas.height = 1200;

            const ctx = canvas.getContext("2d");
            if (!ctx) throw new Error("Canvas context unavailable");

            ctx.fillStyle = "#ffffff";
            ctx.fillRect(0, 0, canvas.width, canvas.height);

            const ratio = Math.min(canvas.width / image.width, canvas.height / image.height);
            const drawWidth = image.width * ratio;
            const drawHeight = image.height * ratio;
            const dx = (canvas.width - drawWidth) / 2;
            const dy = (canvas.height - drawHeight) / 2;
            ctx.drawImage(image, dx, dy, drawWidth, drawHeight);

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
            if (svgUrl) {
                URL.revokeObjectURL(svgUrl);
            }
            exportButton.disabled = false;
            exportButton.textContent = "Export as Image";
        }
    };

    exportButton?.addEventListener("click", exportCalendar);
});
