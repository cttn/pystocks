import pandas as pd
import numpy as np
from dbstocks import dbstocks
import matplotlib.pyplot as plt

dbs = dbstocks()

#  BBAR
bbar = dbs.get_prices("bbar", start="1991-01-01")

#  CCL
ypf = dbs.get_prices("YPFD", start="1991-01-01")
ypfu = dbs.get_prices("YPF_usa", start="1991-01-01")
ccl = ypf.close.div(ypfu.close)
ccl = ccl.to_frame().fillna(method='ffill')

#  BBAR CAP
datos = {}
datos['cap'] = [92.959, 111.499, 111.499, 147.454, 147.454, 186.631, 209.631,
                209.631, 209.361, 209.631, 209.361, 364.631, 471.361, 471.361,
                471.361, 471.361, 536.36, 536.36, 536.878, 536.878, 536.878,
                536.878, 536.878, 536.878, 612.659, 612.659, 612.659, 612.659]
datos['mean'] = list(bbar.close.div(ccl.close).dropna().resample("A").mean())
datos['min'] = list(bbar.close.div(ccl.close).dropna().resample("A").min())
datos['max'] = list(bbar.close.div(ccl.close).dropna().resample("A").max())
datos['mean_h'] = list(bbar.close_h.div(ccl.close).dropna().resample("A").mean())
datos['min_h'] = list(bbar.close_h.div(ccl.close).dropna().resample("A").min())
datos['max_h'] = list(bbar.close_h.div(ccl.close).dropna().resample("A").max())

datos_bbar = pd.DataFrame(datos, index=list(range(1993, 2021)))
e_bbar = datos_bbar.mul(datos_bbar.cap, axis=0)
del e_bbar['cap']

#  plot
plt.clf()
plt.style.use("seaborn")
plt.plot(e_bbar['mean'], linewidth=2, label="Cotización BBAR")
plt.fill_between(e_bbar['mean'].index, e_bbar['min'],
                 e_bbar['max'], color='b', alpha=0.3)
plt.title("BBAR: Cotización histórica de TODO el equity desde 1993")
plt.ylabel("Millones de dólares")
#  plt.plot(np.linspace(1993, 2021, 1000),
#         800*np.sin(2*np.pi*np.linspace(1993, 2021, 1000)/4)+1000,
#         'k--', label="Ciclo 4 años")
plt.savefig("BBAR_ciclox.png")
#  plt.show()
