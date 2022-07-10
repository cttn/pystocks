import pandas as pd
import numpy as np
import datetime as dt
from pystocks.dbstocks import DBstocks
import matplotlib.pyplot as plt


def get_uscpi():
    uscpi = pd.read_excel("examples/CPIAUCNS.xls", skiprows=10)
    uscpi.columns = ['rawdate', 'cpi']
    uscpi['date'] = pd.to_datetime(uscpi['rawdate'], format="%Y-%m-%d")
    del uscpi['rawdate']
    uscpi.set_index("date", inplace=True)
    uscpi = uscpi.append(pd.DataFrame(index=[dt.datetime.now()])).fillna(method="ffill").resample("M").ffill()
    uscpi = uscpi.resample("A").last()
    uscpi['year'] = [i.year for i in uscpi.index]
    uscpi.set_index("year", inplace=True)
    uscpi = uscpi.loc[1993:]
    uscpi = uscpi.div(uscpi.cpi.iloc[-1], axis=0)
    return uscpi

dbs = DBstocks()

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
                536.878, 536.878, 536.878, 612.659, 612.659, 612.659, 612.659,
                612.659, 612.659]
datos['mean'] = list(bbar.close.div(ccl.close).dropna().resample("A").mean())
datos['min'] = list(bbar.close.div(ccl.close).dropna().resample("A").min())
datos['max'] = list(bbar.close.div(ccl.close).dropna().resample("A").max())
datos['mean_h'] = list(bbar.close_h.div(ccl.close).dropna().resample("A").mean())
datos['min_h'] = list(bbar.close_h.div(ccl.close).dropna().resample("A").min())
datos['max_h'] = list(bbar.close_h.div(ccl.close).dropna().resample("A").max())

datos_bbar = pd.DataFrame(datos, index=list(range(1993, 2023)))
e_bbar = datos_bbar.mul(datos_bbar.cap, axis=0)
del e_bbar['cap']

# CPI
uscpi = get_uscpi()
e_bbar_cpi = e_bbar.div(uscpi.cpi, axis=0)

#  plot
plt.clf()
plt.style.use("seaborn")
plt.plot(e_bbar['mean'], linewidth=2, label="Cotización BBAR")
plt.fill_between(e_bbar['mean'].index, e_bbar['min'],
                 e_bbar['max'], color='b', alpha=0.3)
plt.title("BBAR: Cotización histórica de TODO el equity desde 1993")
plt.ylabel("Millones de dólares")

#  Ciclo
#liny = np.linspace(1993,2022,1000)
#periodo = 8
#fase = 0.7

#plt.plot(liny, 800*np.sin(fase*np.pi + 2*np.pi*liny/float(periodo))+1000, 'k--',
#         label="Ciclo " +str(periodo)+ " años")
plt.savefig("BBARhist.png")
#plt.show()


#  plot
plt.clf()
plt.style.use("seaborn")
plt.plot(e_bbar_cpi['mean'], linewidth=2, label="Cotización BBAR")
plt.fill_between(e_bbar_cpi['mean'].index, e_bbar_cpi['min'],
                 e_bbar_cpi['max'], color='b', alpha=0.3)
plt.title("BBAR: Cotización de TODO el equity (ajustado por inflación US)")
plt.ylabel("Millones de dólares ajustado por inflación US")
plt.savefig("BBARhist_cpi.png")