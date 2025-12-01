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
                row.innerHTML = `
                    <p class="incident-title">${inc.inc_number || "Incident"}</p>
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
});
