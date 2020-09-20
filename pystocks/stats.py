import pandas as pd
import numpy as np
from pystocks.dbstocks import DBstocks
import matplotlib.pyplot as plt


class DBstats:
    """Example usage of dbstocks, make some basic stats"""

    def __init__(self):

        #  DB handle
        self.dbs = DBstocks()

        #  Data for selected tickers.
        self.data = None

        #  CCL provided by dbstocks.
        self.ccl = None

        #  Data in USD dollars for selected tickers.
        self.data_usd = None

        #  Dict with data selections.
        self.since = {}

    def get_yprices(self, adjusted=True, crop_VALO=True, del_suspects=True):
        """ Populates self.data with a DataFrame"""

        #  Use adjusted values?
        mycol = "close"
        if adjusted:
            mycol = mycol + "_h"

        #  Get all up to date prices
        precs = []
        for i in self.dbs.dtickers['y']:
            _ = self.dbs.get_prices(i, start="1991-01-01")
            precs.append(_.close_h)

        #  Make a DataFrame with all the prices
        df = pd.concat(precs, axis=1)
        df.columns = self.dbs.dtickers['y']
        self.data = df

        #  Discard VALO data before BYMA spinoff
        if crop_VALO:
            self.data.VALO.loc[:'2017-08-07'] = np.nan

        #  Possible cleanup needed for these tickers.
        if del_suspects:
            del self.data['GAMI']
            del self.data['GCLA']
            del self.data['CGPA2']
            print("WARN: deleted data for GAMI, GCLA, CGPA2")
        return None

    def update(self, update_db=False):
        """ Populates self.data, self.data_usd, self.since, self.ccl """

        #  Do I need to update the DB?
        if update_db:
            self.dbs.update_db()
        else:
            print("WARN: DB not updated.")

        #  Populates self.data
        self.get_yprices()

        #  Populates self.ccl and self.data_usd
        self.get_ccl()

        #  Populates self.since
        self.compute_var_since()
        return None

    def get_ccl(self):
        """ Populates self.ccl and self.data_usd """
        self.ccl = self.dbs.get_ccl()
        self.data_usd = self.data.div(self.ccl.close, axis=0)
        return None

    def compute_var_since(self, start='2017-01-01'):
        """Populates self.since dict"""
        data = self.data_usd.loc[start:]

        #  Var since max
        self.since['max'] = (100. *
                             data.div(data.max(),
                                      axis=1
                                      ).subtract(1)
                             ).dropna().iloc[-1].sort_values()

        #  Var since min
        self.since['min'] = (100. *
                             data.div(data.min(),
                                      axis=1
                                      ).subtract(1)
                             ).dropna().iloc[-1].sort_values()

        #  Var sin PASO
        self.since['paso'] = (100. *
                              data.div(data.loc['2019-08-11':].dropna().iloc[0],
                                       axis=1
                                       ).subtract(1)
                              ).dropna().iloc[-1].sort_values()
        return None

    def graph_barh(self, kind="max"):
        """ Make desired barh plots """
        tit = None
        if kind == 'max':
            tit = "Caída en dólares desde máximos (%)"
        elif kind == 'min':
            tit = "Suba en dólares desde mínimos (%)"
        elif kind == 'paso':
            tit = "Caída en dólares desde las PASO (%)"
        else:
            return SystemExit("[Error] graph_barh: unknown kind")

        #  assign selection to res variable
        res = self.since[kind]

        #  Clear matplotlib garbage
        plt.clf()
        ax = res.plot(kind='barh', grid=True, title=tit, figsize=(8, 16))
        for p in ax.patches:
            ax.annotate(str(np.round(p.get_width(), 1)),
                        (p.get_width()*1.05, p.get_y()))
        return ax

    def graph_dist(self):
        """ Make a smoothed gaussian plot with dollar returns of
        self.since ..."""

        #  Data for the plot
        data = pd.DataFrame(self.since)
        data.columns = ["Desde máximos", "Desde mínimos", "Desde las PASO"]

        #  Clear matplotlib garbage
        plt.clf()

        #  Plot
        ax = data.plot.kde(bw_method="silverman",
                           grid=True,
                           title="Distribución de resultados en dólares",
                           color=['blue', 'orange', 'green'])

        #  Get lines
        lmin = ax.lines[1]
        lmax = ax.lines[0]
        lpaso = ax.lines[2]

        #  Fill under lines
        ax.fill_between(lmax.get_xydata()[:, 0],
                        lmax.get_xydata()[:, 1],
                        color="blue",
                        alpha=0.3)
        ax.fill_between(lmin.get_xydata()[:, 0],
                        lmin.get_xydata()[:, 1],
                        color="orange",
                        alpha=0.3)
        ax.fill_between(lpaso.get_xydata()[:, 0],
                        lpaso.get_xydata()[:, 1],
                        color="green",
                        alpha=0.3)
        return ax
