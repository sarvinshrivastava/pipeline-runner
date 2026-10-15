-- Backs the per-client limit on on-demand runs in flight (MAX_PENDING_PER_CLIENT).
CREATE INDEX runs_in_flight_by_client ON runs(client_id)
  WHERE status IN ('pending', 'running', 'waiting') AND trigger <> 'schedule';
