# pystocks
Pequeña librería de python (en progreso) para manejo, resgurado local y consultas de una base de datos
con precios históricos de acciones argentinas que cotizan (o cotizaron) en la bolsa de Argentna.

La base de datos (sqlite) posee información desde 1993, y la librería permite actualizar fácilmente
con los datos disponibles en Yahoo Finance.

Por el momento, consta de los siguientes archivos:

#### db/dbprices.db
Base de datos (sqlite) con información sobre cotizantes argentinas desde 1993. Es fácilmente actualizable
en forma automática utilizando la clase dbstocks.

#### dbstocks.py (in progress)
Encapsula la base de datos. Permite actualizarla y obtener datos históricos.

#### examples/
Ejemplos funcionales de uso de dbstocks.py

## Usage
Este es un proyecto en fase inicial. Aún se necesita bastante limpieza del código.
Sin embargo, la versión actual funciona bien consultas simples y actualización de la base de datos.

```python
# Uso de dbstocks.py
from pystocks.dbstocks import DBstocks
dbs = DBstocks()

#  Actualizar automáticamente los tickers para los cuales Yahoo finance tiene datos.
dbs.update()  

#Obtiene un DataFrame con todos los datos disponibles para $BBAR desde 1991.
bbar = dbs.get_prices("bbar", start="1991-01-01") 

# Uso de stats.py
from pystocks.stats import DBstats
stats = DBstats()

# Obtener datos desde la DB
stats.update()
#stats.update(update_db=True) # realiza una actualizacion de la DB antes de asignar los datos a stats

# Graficar distribución suavizada
ax = stats.graph_dist()
plt.show()

# Graficar Variación desde las PASO
ax = stats.graph_barh(kind="paso")
plt.show()
```

## Examples
```bash
cd pystocks
ipython
%run examples/bbar_equity_history.py
```
