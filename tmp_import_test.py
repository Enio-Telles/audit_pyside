import sys
sys.path.insert(0, 'src')
try:
    import transformacao.calculos_mensais_pkg.calculos_mensais as m
    print('OK')
except Exception as e:
    import traceback
    traceback.print_exc()
    print('FAILED', e)
