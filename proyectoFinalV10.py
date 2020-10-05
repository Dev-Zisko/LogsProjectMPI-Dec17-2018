#!/usr/bin/env python
from mpi4py import MPI
from time import time
import geoip2.database
import glob


#Obtenemos el tiempo de inicio
timestart = time()

#Inicializamos el comm world
comm = MPI.COMM_WORLD

#Sacamos los ranks de cada proceso
rank = comm.rank

#Obtenemos el size de todos los procesos
size = comm.Get_size()

#Vectormaestro
vectormaster = []

#Vectoresblocksmaestros
vectorblockipsmaster = []
countblockipsmaster = []
vectorblockcountriesmaster = []
countblockcountriesmaster = []
vectorblockcitiesmaster = []
countblockcitiesmaster = []
vectorblockhoursmaster = []
countblockhoursmaster = []
vectorblockaccountsmaster = []
countblockaccountsmaster = []
vectorblockprotocolsmaster = []
countblockprotocolsmaster = []

#Vectoresmaestros y contadoresmaestros
vectoripsmaster = []
countipsmaster = []
vectoripslocalmaster = []
countipslocalmaster = []
vectorcountriesmaster = []
countcountriesmaster = []
vectorcitiesmaster = []
countcitiesmaster = []
vectorhoursmaster = []
counthoursmaster = []
vectoraccountsmaster = []
countaccountsmaster = []
vectorprotocolsmaster = []
countprotocolsmaster = []
rankingcountries = []
rankingcities = []
rankingips = []
rankingipslocal = []
rankinghours = []
rankingaccounts = []
rankingprotocols = []

#Inicializamos el fragment
fragment = 0

#Inicializamos el countblock
countblock = 0

#Ruta de carpeta de logs
pathLogs = glob.glob('/home/jgcafaro.15/logs/logsprueba/*')

#Definicion de funciones

#Funcion que ve cuantos procesos son y crea los vectores a usar
def fragmentFun(size):
  global fragment
  whilefrag = 0
  while(whilefrag<size):
    vectormaster.append([])
    whilefrag += 1
  fragment = size - 1

#Funcion que lee los logs, y coloca linea a linea separandolas en un vector
def readlogFun(txt, lenght):
  global fragment
  whileread = 0
  countfrag = 0
  #Fragmentar el mensaje
  while(whileread<lenght):
    if(countfrag > fragment):
      countfrag = 0
    text = txt.readline()
    vectormaster[countfrag].append(text)
    countfrag += 1
    whileread += 1

#Funcion que lee los logs, y coloca por partes iguales seguidas en un vector
def readlogFun1(txt, lenght):
  global fragment
  limit = lenght / int(fragment + 1)
  whileread = 0
  while(whileread<=fragment):
    whilelimit = 0
    while(whilelimit<limit):
      text = txt.readline()
      vectormaster[whileread].append(text)
      whilelimit += 1
    if(whileread == fragment and txt.readline() != ''):
      text = txt.readline()
      vectormaster[whileread].append(text)
    whileread += 1

#Funcion que organiza la data extraida
def organizeData(vector, master, count):
  lenght = len(vector)
  whilemetrics = 0
  while(whilemetrics<lenght):
    vectortest = vector[whilemetrics]
    lenghttest = len(vectortest)
    whiletest = 0
    while(whiletest<lenghttest):
      repeat = vectortest[whiletest]
      if repeat in master:
        index = master.index(repeat)
        count[index] += 1
      else:
        master.append(repeat)
        count.append(1)
      whiletest += 1
    whilemetrics += 1

#Funcion que verifica el numero de bloques
def promData(vector, master, count):
  global countblock
  lenght = len(vector)
  whilemetrics = 0
  statemark = 0
  state = 0
  while(whilemetrics<lenght):
    vectortest = vector[whilemetrics]
    lenghttest = len(vectortest)
    whiletest = 0
    while(whiletest<lenghttest):
      repeat = vectortest[whiletest]
      if(repeat in master):
        if(state == 1):
          index = master.index(repeat)
          count[index] += 1
          state = 0
      else:
        if(repeat == "-"):
          countblock += 1
          state = 1
        else:
          master.append(repeat)
          count.append(1)
      whiletest += 1
    whilemetrics += 1

#Funcion que organiza la data extraida de los promedios
def promData1(vector, master, count):
  lenght = len(vector)
  whilemetrics = 0
  state = 0
  while(whilemetrics<lenght):
    vectortest = vector[whilemetrics]
    lenghttest = len(vectortest)
    whiletest = 0
    while(whiletest<lenghttest):
      repeat = vectortest[whiletest]
      if(repeat in master):
        if(state == 1):
          index = master.index(repeat)
          count[index] += 1
          state = 0
      else:
        if(repeat == "-"):
          state = 1
        else:
          master.append(repeat)
          count.append(1)
      whiletest += 1
    whilemetrics += 1

#Funcion que ordena los vectores de mayor a menor
def ordertop(master, count):
  for i in range(len(count)-1,0,-1):
    for j in range(i):
      if count[j]>count[j+1]:
        temp = count[j]
        temp1 = master[j]
        count[j] = count[j+1]
        master[j] = master[j+1]
        count[j+1] = temp
        master[j+1] = temp1
  i = 0
  j = len(count) - 1
  temp = []
  temp1 = []
  while(i<len(count)):
    temp.append(count[j])
    temp1.append(master[j])
    i += 1
    j -= 1
  i = 0
  while(i<len(count)):
    master[i] = temp1[i]
    count[i] = temp[i]
    i += 1

#Funcion que muestra los Tops 20 normales
def viewtop(master, count):
  whilevalidate = 0
  whilepercent = 0
  suma = 0
  lenght = len(count)
  if(lenght>20):
    lenght = 20
  while(whilepercent<len(count)):
    suma = suma + count[whilepercent]
    whilepercent += 1
  while(whilevalidate<lenght):
    percent = round((count[whilevalidate]/float(suma))*100,2)
    if(count[whilevalidate]<=2):
      whilevalidate += 1
    else:
      print("{0}.- {1} - Numero de Ataques: {2} - Porcentaje: {3}%".format(str(whilevalidate+1),master[whilevalidate],str(count[whilevalidate]),str(percent)))
    whilevalidate += 1

#Funcion que muestra los Tops 20 en promedio
def viewtop(master, count):
  whilevalidate = 0
  whilepercent = 0
  suma = 0
  lenght = len(count)
  if(lenght>20):
    lenght = 20
  while(whilepercent<len(count)):
    suma = suma + count[whilepercent]
    whilepercent += 1
  while(whilevalidate<lenght):
    percent = round((count[whilevalidate]/float(suma))*100,2)
    if(count[whilevalidate]<=2):
      whilevalidate += 1
    else:
      print("{0}.- {1} - Promedio de Ataques: {2} - Porcentaje: {3}%".format(str(whilevalidate+1),master[whilevalidate],str(count[whilevalidate]),str(percent)))
    whilevalidate += 1

#Empezamos el MPI
if(rank == 0):
  fragmentFun(size)
  whilevali = len(pathLogs)
  whileval = 0
  while(whileval<whilevali):
    txt = open(pathLogs[whileval], "r")
    lenght = len(open(pathLogs[whileval]).readlines())
    readlogFun1(txt, lenght)
    whileval += 1
  txt = open("inputmins", "r")
  screenmins = txt.readline()
  #txt.close()
else:
  vectormaster = None
  screenmins = 0


#Enviar los minutos a todos los procesos
msgbcast = comm.bcast(screenmins, root=0)

#Enviar el mensaje a todos los procesos
msgscatter = comm.scatter(vectormaster, root=0)

#Se procesa la data con todos los procesos
vectorgeneral = []
xmins = int(msgbcast)
prevmins = 0
validatefirts = 0
lenght = len(msgscatter)
whilemetrics = 0
vectorips = []
vectoripslocal = []
vectorcities = []
vectorcountries = []
vectorhours = []
vectoraccounts = []
vectorprotocols = []
vectoravgips = []
vectoravgcities = []
vectoravgcountries = []
vectoravghours = []
vectoravgaccounts = []
vectoravgprotocols = []
while(whilemetrics<lenght):
  data = msgscatter[whilemetrics]
  validationinfo = data[24:28]
  if (validationinfo == "WARN"):
    try:
      reader = geoip2.database.Reader('./GeoLite2-City_20181113/GeoLite2-City.mmdb')
      cutip1 = data.split("oip=")
      cutip2 = cutip1[1].split(";")
      ip = cutip2[0]
      hour = data[11:13]
      mins = data[14:16]
      cutaccount1 = data.split("account=")
      cutaccount2 = cutaccount1[1].split(";")
      account = cutaccount2[0]
      cutprotocol1 = data.split("protocol=")
      cutprotocol2 = cutprotocol1[1].split(";")
      protocol = cutprotocol2[0] 
      validationiplocal = ip[0:7]
      if(validationiplocal == "192.168"):
        vectoripslocal.append(ip)
        vectorcities.append("localhost")
        vectorcountries.append("localhost")
        vectorhours.append(hour)
        vectoraccounts.append(account)
        vectorprotocols.append(protocol)
      else:
        response = reader.city(ip)
        country = format(response.country.name)
        city = format(response.city.name)
        if(city == "None"):
          city = country
        vectorcities.append(city)
        vectorcountries.append(country)
        vectorips.append(ip)
        vectorhours.append(hour)
        vectoraccounts.append(account)
        vectorprotocols.append(protocol)
        if(validatefirts==0):
          genmins = int(mins) + xmins
          validatefirts += 1
        if(prevmins>int(mins)):
          genmins = int(mins) + xmins
          vectoravgips.append("-")
          vectoravgcities.append("-")
          vectoravgcountries.append("-")
          vectoravghours.append("-")
          vectoravgaccounts.append("-")
          vectoravgprotocols.append("-")
        if(int(mins)>genmins):
          genmins = int(mins) + xmins
          vectoravgips.append("-")
          vectoravgcities.append("-")
          vectoravgcountries.append("-")
          vectoravghours.append("-")
          vectoravgaccounts.append("-")
          vectoravgprotocols.append("-")
        if(genmins>60 and 60-int(mins)<=genmins-60):
          vectoravgips.append(ip)
          vectoravgcities.append(city)
          vectoravgcountries.append(country)
          vectoravghours.append(hour)
          vectoravgaccounts.append(account)
          vectoravgprotocols.append(protocol)
        if(genmins>60 and 60-int(mins)>genmins-60):
          genmins = genmins - 60
          vectoravgips.append(ip)
          vectoravgcities.append(city)
          vectoravgcountries.append(country)
          vectoravghours.append(hour)
          vectoravgaccounts.append(account)
          vectoravgprotocols.append(protocol)     
        if(int(mins)<=genmins):
          vectoravgips.append(ip)
          vectoravgcities.append(city)
          vectoravgcountries.append(country)
          vectoravghours.append(hour)
          vectoravgaccounts.append(account)
          vectoravgprotocols.append(protocol)    
        prevmins = int(mins)
        reader.close()
    except IndexError:
      pass
  whilemetrics += 1

#Recibir la data procesada en el nodo maestro
msgvectorips = comm.gather(vectorips,root=0)
msgvectoripslocal = comm.gather(vectoripslocal,root=0)
msgvectorcountries = comm.gather(vectorcountries,root=0)
msgvectorcities = comm.gather(vectorcities,root=0)
msgvectorhours = comm.gather(vectorhours,root=0)
msgvectoraccounts = comm.gather(vectoraccounts,root=0)
msgvectorprotocols = comm.gather(vectorprotocols,root=0)

msgvectoravgips = comm.gather(vectoravgips,root=0)
msgvectoravgcountries = comm.gather(vectoravgcountries,root=0)
msgvectoravgcities = comm.gather(vectoravgcities,root=0)
msgvectoravghours = comm.gather(vectoravghours,root=0)
msgvectoravgaccounts = comm.gather(vectoravgaccounts,root=0)
msgvectoravgprotocols = comm.gather(vectoravgprotocols,root=0)

#Organizar todo y sacar metricas nodo maestro
if(rank==0):
  organizeData(msgvectorips, vectoripsmaster, countipsmaster)
  organizeData(msgvectoripslocal, vectoripslocalmaster, countipslocalmaster)
  organizeData(msgvectorcountries, vectorcountriesmaster, countcountriesmaster)
  organizeData(msgvectorcities, vectorcitiesmaster, countcitiesmaster)
  organizeData(msgvectorhours, vectorhoursmaster, counthoursmaster)
  organizeData(msgvectoraccounts, vectoraccountsmaster, countaccountsmaster)
  organizeData(msgvectorprotocols, vectorprotocolsmaster, countprotocolsmaster)
  ordertop(vectoripsmaster,countipsmaster)
  ordertop(vectoripslocalmaster,countipslocalmaster)
  ordertop(vectorcountriesmaster,countcountriesmaster)
  ordertop(vectorcitiesmaster,countcitiesmaster)
  ordertop(vectorhoursmaster,counthoursmaster)
  ordertop(vectoraccountsmaster,countaccountsmaster)
  ordertop(vectorprotocolsmaster,countprotocolsmaster)
  promData(msgvectoravgips, vectorblockipsmaster, countblockipsmaster)
  promData1(msgvectoravgcountries, vectorblockcountriesmaster, countblockcountriesmaster)
  promData1(msgvectoravghours, vectorblockhoursmaster, countblockhoursmaster)
  promData1(msgvectoravgaccounts, vectorblockaccountsmaster, countblockaccountsmaster)
  promData1(msgvectoravgprotocols, vectorblockprotocolsmaster, countblockprotocolsmaster)
  promData1(msgvectoravgcities, vectorblockcitiesmaster, countblockcitiesmaster)
  ordertop(vectorblockipsmaster, countblockipsmaster)
  ordertop(vectorblockcountriesmaster, countblockcountriesmaster)
  ordertop(vectorblockcitiesmaster, countblockcitiesmaster)
  ordertop(vectorblockhoursmaster, countblockhoursmaster)
  ordertop(vectorblockaccountsmaster, countblockaccountsmaster)
  ordertop(vectorblockprotocolsmaster, countblockprotocolsmaster)
  print("#######################################################")
  print("#                     METRICAS                        #")
  print("#######################################################")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 PAISES CON MAS ATAQUES            #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorcountriesmaster,countcountriesmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 CIUDADES CON MAS ATAQUES          #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorcitiesmaster,countcitiesmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 IPS MAS ATACADAS                  #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectoripsmaster,countipsmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 HORAS MAS ATACADAS                #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorhoursmaster,counthoursmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 CUENTAS MAS ATACADAS              #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectoraccountsmaster,countaccountsmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP PROTOCOLOS MAS ATACADOS              #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorprotocolsmaster, countprotocolsmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            ATAQUES DESDE IP DE LOCALHOST            #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectoripslocalmaster,countipslocalmaster)
  print("########################################################")
  print("#CADA {0} MINUTOS SE ENCONTRARON UN TOTAL DE: {1} BLOQUES DE ATAQUE.#".format(str(screenmins),str(countblock)))
  print("########################################################")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 PAISES CON MAS ATAQUES            #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorblockcountriesmaster,countblockcountriesmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 CIUDADES CON MAS ATAQUES          #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorblockcitiesmaster,countblockcitiesmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 IPS MAS ATACADAS                  #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorblockipsmaster,countblockipsmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 HORAS MAS ATACADAS                #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorblockhoursmaster,countblockhoursmaster)
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  print("#            TOP 20 CUENTAS MAS ATACADAS              #")
  print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
  viewtop(vectorblockaccountsmaster,countblockaccountsmaster)
timeelapsed = int(time()) - int(timestart)
print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")
print("EL TIEMPO DE EJECUCION PARA EL PROCESO {1} ES: {0} SEGS.".format(str(timeelapsed),str(rank)))
print("#+-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+-++-+-+-+-+#")

