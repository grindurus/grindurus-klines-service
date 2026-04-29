from datetime import datetime, timedelta

from app.adapters import adapter_registry

symbols_cache = {}
"""dictionary to store dictionaries ('symbols': [symbols], 'creation_date': date)"""


def get_symbols(exchange: str):
    data_present_in_cache = symbols_cache.get(exchange)

    # If it's not in cache OR it has expired, fetch new data
    if not data_present_in_cache or expired(data_present_in_cache):
        exchange_adapter = adapter_registry.get_adapter(exchange)

        symbols = exchange_adapter.get_available_symbols()

        symbols_cache[exchange] = {
            'creation_date': datetime.now(),
            'symbols': symbols
        }
        data_present_in_cache = symbols_cache[exchange]

    return data_present_in_cache['symbols']
def expired(symbols):
    return symbols['creation_date'] < (datetime.now() - timedelta(days=1))
