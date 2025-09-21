import os
from dataclasses import dataclass

# ==== загружаем .env c override ====
try:
    from dotenv import load_dotenv
    # можно переопределить путь к .env через переменную ENV_FILE, иначе берём ./ .env
    load_dotenv(dotenv_path=os.getenv("ENV_FILE", ".env"), override=True)
except Exception:
    # dotenv не установлен — просто работаем с тем что есть в окружении
    pass

# ==== робастные парсеры ====
def _clean_env(s: str | None) -> str | None:
    if s is None:
        return None
    # срезаем комментарий после '#'
    s = s.split('#', 1)[0].strip()
    # берём только первый токен, если после значения есть мусор (например "0.10 .")
    if ' ' in s:
        s = s.split()[0]
    # запятая -> точка
    s = s.replace(',', '.')
    return s or None

def env_int(name: str, default: int) -> int:
    raw = _clean_env(os.getenv(name))
    if raw is None:
        return default
    try:
        return int(float(raw))
    except Exception:
        return default

def env_float(name: str, default: float) -> float:
    raw = _clean_env(os.getenv(name))
    if raw is None:
        return default
    try:
        return float(raw)
    except Exception:
        return default

def env_bool(name: str, default: int | bool) -> int:
    raw = _clean_env(os.getenv(name))
    if raw is None:
        return int(default)
    v = raw.lower()
    if v in ("1", "true", "t", "yes", "y", "on"):
        return 1
    if v in ("0", "false", "f", "no", "n", "off"):
        return 0
    try:
        return int(float(raw))
    except Exception:
        return int(default)

def env_str(name: str, default: str) -> str:
    raw = _clean_env(os.getenv(name))
    return raw if raw is not None else default


@dataclass
class Settings:
    SYMBOL: str = env_str("SYMBOL", "ETHUSDT")
    TF_D1: str = env_str("TF_D1", "1d")
    TF_H4: str = env_str("TF_H4", "4h")
    TF_H1: str = env_str("TF_H1", "1h")

    # Тренд
    D1_SMA: int = env_int("D1_SMA", 200)
    H4_FAST: int = env_int("H4_FAST", 50)
    H4_SLOW: int = env_int("H4_SLOW", 200)
    STRICT_TREND: int = env_bool("STRICT_TREND", 0)

    # Фейкаут H1
    H1_FALSE_BREAKOUT_MAX_CANDLES: int = env_int("H1_FALSE_BREAKOUT_MAX_CANDLES", 3)
    H1_MAX_PENETRATION_ATR_PCT: float = env_float("H1_MAX_PENETRATION_ATR_PCT", 0.30)
    H1_RETEST_MAX_CANDLES: int = env_int("H1_RETEST_MAX_CANDLES", 4)

    # Пробой в процентах
    PENETRATION_MIN_PCT: float = env_float("PENETRATION_MIN_PCT", 0.001)
    PENETRATION_MAX_PCT: float = env_float("PENETRATION_MAX_PCT", 0.007)
    CLOSE_BACK_MAX_PCT: float = env_float("CLOSE_BACK_MAX_PCT", 0.003)

    # ATR / допуски
    ATR_PERIOD_H1: int = env_int("ATR_PERIOD_H1", 14)
    ENTRY_RETEST_TOLERANCE_ATR_PCT: float = env_float("ENTRY_RETEST_TOLERANCE_ATR_PCT", 0.12)
    ENTRY_OFFSET_ATR_PCT: float = env_float("ENTRY_OFFSET_ATR_PCT", 0.08)
    STOP_MODE: str = env_str("STOP_MODE", "wick")  # wick | level
    STOP_BUFFER_ATR_PCT: float = env_float("STOP_BUFFER_ATR_PCT", 0.10)

    # Вход и стопы по процентам
    ENTRY_MODE: str = env_str("ENTRY_MODE", "next_open")
    ENTRY_UNDERFILL_PCT: float = env_float("ENTRY_UNDERFILL_PCT", 0.001)
    STOP_WICK_TICKS: int = env_int("STOP_WICK_TICKS", 3)
    STOP_LEVEL_PCT: float = env_float("STOP_LEVEL_PCT", 0.005)

    # Объёмы / уровни
    VOLUME_MA_LENGTH: int = env_int("VOLUME_MA_LENGTH", 20)
    VOLUME_MAX_RATIO: float = env_float("VOLUME_MAX_RATIO", 0.8)
    LEVEL_LOOKBACK_DAYS: int = env_int("LEVEL_LOOKBACK_DAYS", 10)

    # MM / RM
    RR: float = env_float("RR", 3.0)
    RISK_PCT: float = env_float("RISK_PCT", 0.01)
    MAX_CONSECUTIVE_LOSSES: int = env_int("MAX_CONSECUTIVE_LOSSES", 3)

    # Ограничения позиций
    MAX_OPEN_POSITIONS: int = env_int("MAX_OPEN_POSITIONS", 3)

    # Защиты
    ONE_TRADE_PER_DAY: int = env_bool("ONE_TRADE_PER_DAY", 1)
    DEDUP_BY_BREAKOUT: int = env_bool("DEDUP_BY_BREAKOUT", 1)
    COOLDOWN_HOURS: int = env_int("COOLDOWN_HOURS", 8)

    # Аналитика / режим
    INCLUDE_TODAY: int = env_bool("INCLUDE_TODAY", 1)

    # Режимы/логи/планировщик
    DRY_RUN: int = env_bool("DRY_RUN", 1)
    LOGLEVEL: str = env_str("LOGLEVEL", "INFO")
    RUN_EVERY_SEC: int = env_int("RUN_EVERY_SEC", 60)
    ENABLE_SCHEDULER: int = env_bool("ENABLE_SCHEDULER", 0)

    # Сканер (опционально)
    SCAN_SYMBOLS: str = env_str("SCAN_SYMBOLS", "")


settings = Settings()
