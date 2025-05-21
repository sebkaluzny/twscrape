import os
from prometheus_client import Counter, Gauge, start_http_server, Histogram,CollectorRegistry

metrics_reg = CollectorRegistry()

# --- API Calls ---
api_calls_total = Counter(
    "twscrape_api_calls_total",
    "Total number of API calls made",
    ["operation", "method", "status_code", "outcome"],
    # outcome: success, http_error, ratelimited, ban_detected, dependency_error, auth_error, missing_content, unknown_api_error, handled_error_retry, aborted_request, network_error_retry
    registry=metrics_reg,
)

api_call_latency = Histogram(
    "twscrape_api_call_latency_seconds",
    "Latency of API calls",
    ["operation", "method"],
    registry=metrics_reg,
)

# --- Account Pool ---
account_pool_requests_total = Counter(
    "twscrape_account_pool_requests_total",
    "Total number of requests to the account pool for an account",
    ["queue", "status"],  # status: success, no_account_available, waiting, no_account_raised
    registry=metrics_reg,
)

account_status_gauge = Gauge(
    "twscrape_account_status_count",
    "Number of accounts by status",
    ["status"],  # status: active, inactive, error
    registry=metrics_reg,
)

account_locks_gauge = Gauge(
    "twscrape_account_locks_per_queue_count",
    "Number of locked accounts by queue",
    ["queue"],
    registry=metrics_reg,
)
# Store known queues to handle labels that might disappear
_known_lock_queues = set()


# --- Login Attempts ---
login_attempts_total = Counter(
    "twscrape_login_attempts_total",
    "Total number of login attempts",
    ["username", "status"],
    # status: success, http_error, mfa_error, email_code_error, email_login_error, unknown_error
    registry=metrics_reg,
)

# --- IMAP Operations ---
imap_operations_total = Counter(
    "twscrape_imap_operations_total",
    "Total number of IMAP operations",
    ["operation", "email_domain", "status"],  # operation: login, get_code; status: success, failure, timeout
    registry=metrics_reg,
)

# --- XClId Generation ---
xclid_generation_total = Counter(
    "twscrape_xclid_generation_total",
    "Total number of X-Client-ID generation attempts (create)",
    ["status"],  # status: success, failure
    registry=metrics_reg,
)


def _get_db_file_from_args():
    import sys
    db_file = "accounts.db" # default
    if "--db" in sys.argv:
        try:
            db_file_idx = sys.argv.index("--db") + 1
            if db_file_idx < len(sys.argv):
                db_file = sys.argv[db_file_idx]
        except (ValueError, IndexError):
            pass # use default
    return db_file


_metrics_server_started_flag = False

def start_metrics_http_server(port: int = 8000, addr: str = "0.0.0.0"):
    """Starts an HTTP server to expose Prometheus metrics if not already started."""
    global _metrics_server_started_flag
    if _metrics_server_started_flag:
        return

    try:
        start_http_server(port, addr)
        _metrics_server_started_flag = True
        print(f"Prometheus metrics server started on http://{addr}:{port}/metrics")
    except OSError as e: # pragma: no cover
        # common if port is already in use, e.g. in tests or multiple runs
        print(f"Could not start Prometheus metrics server on {addr}:{port}: {e}")


async def update_account_pool_gauges(pool): # pool is AccountsPool instance
    """Updates gauges related to the account pool by querying the database."""
    if not _metrics_server_started_flag: # Only update if server is running
        return

    # Account Status
    active_q = "SELECT COUNT(*) FROM accounts WHERE active = true AND error_msg IS NULL"
    # Inactive means not active and no specific error registered that took it out of rotation
    inactive_q = "SELECT COUNT(*) FROM accounts WHERE active = false AND error_msg IS NULL"
    error_q = "SELECT COUNT(*) FROM accounts WHERE error_msg IS NOT NULL"

    from .db import fetchone # Local import to avoid circular dependency if metrics is imported early

    active_count = (await fetchone(pool._db_file, active_q))[0]
    inactive_count = (await fetchone(pool._db_file, inactive_q))[0]
    error_count = (await fetchone(pool._db_file, error_q))[0]

    account_status_gauge.labels(status='active').set(active_count)
    account_status_gauge.labels(status='inactive').set(inactive_count)
    account_status_gauge.labels(status='error').set(error_count)

    # Account Locks per queue
    # This part can be complex if we want to clear old labels.
    # Simpler approach: Prometheus will stop seeing labels for queues with no locks.
    # For an active "0" count, we'd need to track all seen queues.
    stats_data = await pool.stats() # uses fetchall internally
    
    # Clear old known queues first by setting them to 0, then update current ones
    # This prevents stale queue lock counts if a queue becomes unused.
    current_queues_with_locks = set()
    for k, v in stats_data.items():
        if k.startswith("locked_"):
            queue_name = k.replace("locked_", "")
            account_locks_gauge.labels(queue=queue_name).set(v)
            _known_lock_queues.add(queue_name)
            current_queues_with_locks.add(queue_name)
    
    queues_to_clear = _known_lock_queues - current_queues_with_locks
    for q_name in queues_to_clear:
        account_locks_gauge.labels(queue=q_name).set(0)
    _known_lock_queues.intersection_update(current_queues_with_locks)