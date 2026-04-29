from app.service import symbols_service

symbols = symbols_service.get_symbols("binance")
print(len(symbols['base']), len(symbols['quote']))
print(symbols)