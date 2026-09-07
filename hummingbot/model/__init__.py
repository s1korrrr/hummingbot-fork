from sqlalchemy.orm import declarative_base

HummingbotBase = declarative_base()


def get_declarative_base():
    from .chart_snapshot import ChartSnapshot  # noqa: F401
    from .controller_candle_watermark import ControllerCandleWatermark  # noqa: F401
    from .controllers import Controllers  # noqa: F401
    from .executors import Executors  # noqa: F401
    from .funding_payment import FundingPayment  # noqa: F401
    from .market_data import MarketData  # noqa: F401
    from .market_state import MarketState  # noqa: F401
    from .metadata import Metadata  # noqa: F401
    from .order import Order  # noqa: F401
    from .order_status import OrderStatus  # noqa: F401
    from .position import Position  # noqa: F401
    from .position_hold_ledger import PositionHoldLedger  # noqa: F401
    from .range_position_collected_fees import RangePositionCollectedFees  # noqa: F401
    from .range_position_update import RangePositionUpdate  # noqa: F401
    from .rsi_v5_runtime_event import RSIV5RuntimeEvent  # noqa: F401
    from .trade_fill import TradeFill  # noqa: F401
    return HummingbotBase
