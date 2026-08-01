// NYC 311 Dashboard — Filter and Export Logic


function applyFilters() {
    var anomalous = document.getElementById('anomalousFilter').value;
    var search    = document.getElementById('searchInput').value.toLowerCase().trim();

    document.querySelectorAll('.merge-row').forEach(function(row) {
        var type        = row.dataset.type || '';
        var isAnomalous = row.dataset.anomalous;
        var show        = true;

        if (anomalous === 'true' && isAnomalous !== 'true') show = false;
        if (search && !type.includes(search)) show = false;

        row.style.display = show ? '' : 'none';
    });
}

function resetFilters() {
    document.getElementById('anomalousFilter').value = 'false';
    document.getElementById('searchInput').value     = '';
    document.querySelectorAll('.merge-row').forEach(function(r) {
        r.style.display = '';
    });
}