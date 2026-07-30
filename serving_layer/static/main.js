// NYC 311 Dashboard — Filter and Export Logic


function applyFilters() {
    const borough   = document.getElementById('boroughFilter').value;
    const severity  = document.getElementById('severityFilter').value;
    const anomalous = document.getElementById('anomalousFilter').value;
    const search    = document.getElementById('searchInput').value.toLowerCase();

    document.querySelectorAll('.merge-row').forEach(row => {
        const type        = row.dataset.type.toLowerCase();
        const isAnomalous = row.dataset.anomalous;
        let show = true;

        if (anomalous === 'true' && isAnomalous !== 'true') show = false;
        if (search && !type.includes(search)) show = false;

        row.style.display = show ? '' : 'none';
    });

    // Update export links with filters
    const params = new URLSearchParams();
    if (anomalous === 'true') params.set('anomalous', 'true');
    if (severity !== 'all')   params.set('severity', severity);

    const queryStr = params.toString();
    document.querySelectorAll('.export-btn').forEach(btn => {
        const base = btn.getAttribute('href').split('?')[0];
        btn.setAttribute('href', queryStr ? `${base}?${queryStr}` : base);
    });
}

function resetFilters() {
    document.getElementById('boroughFilter').value   = 'all';
    document.getElementById('severityFilter').value  = 'all';
    document.getElementById('anomalousFilter').value = 'false';
    document.getElementById('searchInput').value     = '';

    document.querySelectorAll('.merge-row').forEach(r => r.style.display = '');

    document.querySelectorAll('.export-btn').forEach(btn => {
        const base = btn.getAttribute('href').split('?')[0];
        btn.setAttribute('href', base);
    });
}

// Auto refresh countdown
let countdown = 30;
setInterval(() => {
    countdown--;
    if (countdown <= 0) countdown = 30;
}, 1000);