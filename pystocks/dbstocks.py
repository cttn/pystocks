import sqlalchemy as db
from sqlalchemy.orm import sessionmaker
import datetime as dt
import yfinance as yf
import pandas as pd
import requests
import os


class DBstocks:
    """This class encapsulates the db"""

    #  There are a lot more tickers in the DB, since 1993. However, dtickers
    #  only provides those that at this time can be updated with a simple quary
    dtickers = {
                #  BYMA tickers with available data from Y!
                'y': ['ALUA', 'BMA', 'BYMA', 'CEPU', 'COME', 'CRES', 'CVH',
                      'EDN', 'GGAL', 'MIRG', 'PAMP', 'SUPV', 'TECO2', 'TGNO4',
                      'TGSU2', 'TRAN', 'TXAR', 'VALO', 'YPFD', 'DOME', 'AGRO',
                      'AUSO', 'BBAR', 'BHIP', 'BOLT', 'BPAT', 'BRIO', 'CADO',
                      'CAPX', 'CARC', 'CECO2', 'CELU', 'CGPA2', 'CTIO', 'EDSH',
                      'DGCU2', 'DYCA', 'EDLH', 'ESME', 'FERR', 'FIPL', 'GAMI',
                      'GARO', 'GBAN', 'GCLA', 'GRIM', 'HARG', 'HAVA', 'INTR',
                      'INVJ', 'IRCP', 'IRSA', 'LEDE', 'LOMA', 'LONG', 'METR',
                      'MOLA', 'MOLI', 'MORI', 'OEST', 'PATA', 'PGR', 'POLL',
                      'RICH', 'RIGO', 'ROSE', 'SAMI', 'SEMI', 'TGLT'],

                #  NYSE tickers for local stocks with available data from Y!
                'yusa': ['BBAR_usa', 'BMA_usa', 'GGAL_usa', 'TGS_usa',
                         'IRS_usa', 'CRESY_usa', 'SUPV_usa', 'PAM_usa',
                         'YPF_usa', 'CEPU_usa', 'EDN_usa'],

                #  BCRA dolar.
                #  ToDo: Add more dolar series.
                'bcra': ['dolar_bcra_a3500']
                }

    def __init__(self, dbname=None, log=True):

        #  DB PATH
        if dbname is None:
            mdir = os.path.dirname(__file__)
            self.dbname = "sqlite:///" + str(os.path.join(mdir,
                                                           "db",
                                                           "dbprices.db"))

        #  Screen log boolean
        self.log = log

        #  DB handle. ToDo: improve this.
        self.engine, self.connect, self.metadata = self.get_connection()
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def get_connection(self):
        """Returns db handle"""
        engine = db.create_engine(self.dbname)
        connect = engine.connect()
        metadata = db.MetaData()
        return engine, connect, metadata

    def update_db(self, start=None):
        """Updates all the prices in the db"""

        #  Available AR Values from Y!
        if start is None:
            start = self.get_last_date('y')
        print("[db] Updating values from Y! since " + str(start))
        self._upsert_yahoo_data(start=start)

        #  US Values from Y!
        if start is None:
            start = self.get_last_date('yusa')
        print("[db] Updating values from Y! since " + str(start))
        self._upsert_yahoo_data(start=start, category='yusa')

        #  Values from BCRA
        if start is None:
            start = self.get_last_date('bcra')
        print("[db] Updating values from BCRA since " + str(start))
        for fuente in self.dtickers['bcra']:
            self._upsert_dolar_data(fuente=fuente, start=start)

        return None

    def get_last_date(self, category, debug=False):
        """Returns the earliest date not updated in db for
        self.dtickers[category]"""

        last = dt.datetime.now()
        for ticker in self.dtickers[category]:
            try:
                res = self.connect.execute("SELECT date FROM " + str(ticker) +
                                           " ORDER by date DESC LIMIT 1")
                current = dt.datetime.strptime(res.first()[0], "%Y-%m-%d")
                if debug:
                    print(str(ticker) + ": " + str(current))
                if current < last:
                    last = current
            except:
                print("[db] Warning: Table " + str(ticker) + " not found in " +
                      str(self.dbname) + ".")
        last = last + dt.timedelta(days=1)
        return last

    def create_ticker_table(self, ticker):
        """Creates a table of ticker in the db"""
        ticker = ticker.lower()
        emp = db.Table(ticker, self.metadata,
                       db.Column('date', db.Date(), primary_key=True),
                       db.Column('max', db.Float()),
                       db.Column('min', db.Float()),
                       db.Column('close', db.Float()),
                       db.Column('volnom', db.Float()),
                       db.Column('vol', db.Float()),
                       db.Column('start', db.Float()),
                       db.Column('start_h', db.Float()),
                       db.Column('max_h', db.Float()),
                       db.Column('min_h', db.Float()),
                       db.Column('volnom_h', db.Float()),
                       db.Column('vol_h', db.Float()),
                       db.Column('close_h', db.Float()))

        if self.log:
            message = "[db] Creating table for ticker " + str(ticker) + "."
            print(message)

        self.metadata.create_all(self.engine)
        return None

    def _sanitize(self, my_string):
        """All sanitizing needed for strings before sql handling"""
        return my_string if my_string != '' else None

    def get_table(self, ticker):
        """Returns db handle for the ticker table."""

        #  If table don't exist, Create.
        if not self.engine.dialect.has_table(self.engine, ticker):
            if self.log:
                message = ("[db] Table for ticker " + str(ticker) +
                           " does not exist.")
                print(message)
            self.create_ticker_table(ticker)
        my_table = db.Table(ticker, self.metadata, autoload=True,
                            autoload_with=self.engine)
        return my_table

    def _print_table(self, ticker):
        """For debug purposes"""
        ticker_table = self.get_table(ticker)
        return pd.DataFrame(self.connect.execute(
                            db.select([ticker_table])).fetchall())

    def insert_all_into_table(self, table, value_list):
        """Inserts the given list of values into ticker table"""
        my_table = self.get_table(table)
        query = db.insert(my_table)
        results = self.connect.execute(query, value_list)
        return None

    def _upsert_data(self, value_dict):
        """ Inserts data into de db"""

        for ticker in value_dict.keys():
            print("[db] Upserting values from ticker " + str(ticker))
            ticker_table = self.get_table(ticker)
            for value in value_dict[ticker]:
                value_date = value['date'].strftime("%Y-%m-%d")
                exists = self.session.query(ticker_table).filter_by(date=value_date).scalar() is not None
                try:
                    if not exists:
                        query = db.insert(ticker_table)
                        results = self.connect.execute(query, value)
                    else:
                        query = db.update(ticker_table).where(
                                ticker_table.c.date == value_date).values(
<<<<<<< HEAD
                                    {i: value[i] for i in value if i != 'date'})
                        results = self.connect.execute(query, value)
                except:
                    message = "[db] Could not update " + ticker
                    print(message)
=======
                                        {i: value[i] for i in value if i != 'date'})
                        results = self.connect.execute(query, value)
                except:
                    print("Error updateing " + ticker)
>>>>>>> b21b6681667ed89a7f2b7c3294e9166dd29c9481

    def _upsert_yahoo_data(self, start, end=None, category="y"):
        """Return value dict for upsert from yahoo data"""
        if end is None:
            dt.datetime.now().strftime("%Y-%m-%d")
        value_dict = self.get_data_from_yahoo(start=start,
                                              end=end,
                                              category=category)
        self._upsert_data(value_dict)
        return None

    def _upsert_dolar_data(self, fuente="a3500", start=None, end=None):
        """Updates db with dolar data"""
        if fuente.lower() == "a3500":
            value_dict = self.get_dolar_bcra(fuente=fuente,
                                             start=start,
                                             end=end)
            self._upsert_data(value_dict)
        return None

    def get_data_from_yahoo(self, start, end, category='y'):
        """ Encapsulates yf.download()"""
        value_dict = {}
        for ticker in self.dtickers[category]:
            self.myprint("Getting " + str(ticker) + " data from Y!")
            data = yf.download(self.yticker(ticker,
                                            category=category),
                                            start=start,
                                            end=end,
                                            progress=False)
            data.reset_index(inplace=True)
            value_dict[ticker] = []
            for row in data.iterrows():
                value_dict[ticker].append({'close':   row[1].Close,
                                           'max':     row[1].High,
                                           'min':     row[1].Low,
                                           'start':   row[1].Open,
                                           'volnom':  row[1].Volume,
                                           'vol':     None,
                                           'close_h': row[1]['Adj Close'],
                                           'date':    row[1].Date})
        return value_dict

    def yticker(self, ticker, category):
        """Returns proper ticker format for Y!"""
        category = category.lower()
        if category == "y":
            ticker = ticker + ".ba"
        elif category == "yusa":
            ticker = ticker[:-4]
        else:
            raise SystemExit("Category error " + str(category))
        return ticker.lower()

    def get_prices(self, ticker, start, end=None, dt_index=True):
        """Ejemplo para obtener precios de un ticker desde la base de datos"""
        ticker = ticker.lower()
        engine = db.create_engine(self.dbname)

        if end is None:
            end = dt.datetime.now().strftime("%Y-%m-%d")

        query = ("SELECT * FROM " + ticker + " WHERE date between '" + start +
                  "' AND '" + end + "';")

        prices = pd.read_sql(query, con=engine)
        if dt_index:
            fecha = pd.to_datetime(prices.date, format="%Y-%m-%d")
            prices['date'] = fecha
            prices.set_index("date", inplace=True)
        return prices

    def get_ccl(self, start=None, end=None):
        """Computes and return CCL"""
        #ToDo CCL average using alse other tickers
        ypf = self.get_prices("YPFD", start="1991-01-01")
        ypfu = self.get_prices("YPF_usa", start="1991-01-01")
        ccl = ypf.close.div(ypfu.close)
        ccl = ccl.to_frame().fillna(method='ffill')
        return ccl

    def get_dolar_bcra(self, fuente='dolar_bcra_a3500', start=None, end=None):
        """Start and end in %Y%m%d format"""

        fuente = fuente.lower()
        if fuente not in self.dtickers['bcra']:
            print('Opción no reconocida. Opciones: ' + str(opciones))
            return None

        value_dict = {}

        if end is None:
            dt.datetime.now().strftime("%Y%m%d")
        if start is None:
            start = dt.datetime(2002, 3, 4).strftime("%Y%m%d")

        #  Downloading BCRA data
        if fuente == 'dolar_bcra_a3500':
            self.myprint("Dowloading a3500 data from bcra.gov.ar")
            dlr_url = 'http://www.bcra.gov.ar/Pdfs/PublicacionesEstadisticas/com3500.xls'
            datos = requests.get(dlr_url)
            with open("com3500.xls", "wb") as f:
                f.write(datos.content)

            #  Cleaning up
            usd = pd.read_excel("com3500.xls", header=4)
            fecha = usd.iloc[:, 2]
            valor = usd.iloc[:, 3]
            dolar = pd.concat([fecha, valor], axis=1)
            dolar.columns = ['fecha', 'valor']
            dolar.set_index('fecha', inplace=True)
            dolar = dolar.valor.dropna(how='all')
            dolar = dolar.loc[start:end]
            dolar = dolar.to_frame()
            value_dict['dolar_bcra_a3500'] = []
            for row in dolar.iterrows():
                value_dict['dolar_bcra_a3500'].append({'close':row[1].valor,
                                                       'date':row[0]})
        return value_dict

    def myprint(self, string, override=False):
        """If override, prints indepently of log==True or False"""
        if self.log or override:
            print('[db] ' + str(string))
