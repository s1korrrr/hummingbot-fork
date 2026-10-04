"""Constants for the shared candle-service feed; mirrors the admitted OKX spot 1m delivery."""
from bidict import bidict

# Connector name accepted by CandlesFactory. V1 keeps `candles_connector: okx` as its identity namespace
# (watermarks, recorder rows, chart snapshots); the factory routes `okx` here only when the process opted in.
SERVICE_CONNECTOR = "okx_service"

# The shared service and the stream client admit confirmed OKX spot 1m bars only.
INTERVALS = bidict({"1m": "1m"})

# V1's largest read: `ok_rsi_core.context.HISTORY_RECORDS` (owner reconstruction). One process-wide subscription of
# this depth serves every V1 controller (500 base, 1275 derived 5m/15m, 3660 reconstruction).
SERVICE_HISTORY = 3660

# The five Hummingbot controllers of a process attach in the same event-loop turn; one rebuild serves them all.
COALESCE_SECONDS = 0.25
# A feed switch (stop, then create with larger max_records) must not tear the shared stream down.
IDLE_CLOSE_SECONDS = 10.0
