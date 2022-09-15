
#In[67]:
from databases import Database
from fastapi import FastAPI, UploadFile, File, Form


database = Database("sqlite:///curso.db")
app=FastAPI()

@app.on_event("startup")
async def database_connect():
    await database.connect()


@app.on_event("shutdown")
async def database_disconnect():
    await database.disconnect()

@app.get("/Nombre del circuito más corrido")
async def fetch_data(Cantidad_de_Resultados: int):
    Consulta = "SELECT ci.name as Nombre_Circuito,count(ci.circuitId) as Veces_corrido FROM races ra JOIN circuits ci  ON (ra.circuitId = ci.circuitId) group by ci.circuitId order by Veces_corrido desc limit {};".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return  results
@app.get("/Año con mas carreras")
async def fetch_data(Cantidad_de_Resultados: int):
    Consulta = "SELECT year as AÑO,count(circuitId)git exit as Carreras_Totales FROM races group by year order by Carreras_Totales  desc limit {}".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)

    return  results

@app.get("/Piloto con mayor cantidad de primeros puestos")
async def fetch_data(Cantidad_de_Resultados: int):
    Consulta = "SELECT dri.driverRef as Nombre_Piloto,count(positionOrder) as Veces_primer_puesto FROM results re JOIN drivers dri ON (re.driverId = dri.driverId) where positionOrder=1 group by dri.driverId order by Veces_primer_puesto desc limit {}".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return  results

@app.get("/Piloto con mayor cantidad de puntos en total, cuyo constructor sea de nacionalidad sea American o British")
async def fetch_data(Cantidad_de_Resultados: int):
    Consulta = "SELECT driverRef as Piloto,constructorRef as Constructor,nationality as Nacionalidad_Constructor,sum(points) as Puntos_Totales FROM prueba pr JOIN constructors cons ON (pr.constructorId = cons.constructorId) where nationality='British' or nationality='American' group by	driverId order by Puntos_Totales desc limit {}".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)
    return  results
# %%



@app.get("/prueba")
async def fetch_data(Cantidad_de_Resultados: int):
    Consulta = "SELECT year as AÑO,sum(round) as Carreras_Totales FROM races group by year order by Carreras_Totales  desc limit {}".format(str(Cantidad_de_Resultados))
    results = await database.fetch_all(query=Consulta)

    return  results