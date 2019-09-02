# This Python file uses the following encoding: utf-8
"""
 Script para Instalacion y configuracion de HTCondor en Linux
  Edier Alberto Zapata Hernandez edalzap@gmail.com
  Marzo 2010 Version 1.0RC3.1/2k111028
  Migrado a Python: Marzo 2015
  Version 2: Octubre 2018
"""
# GitHub's RAW URL:
# https://raw.githubusercontent.com/edza978/htconfig/master/htconfig.py

# Manejo de argumentos
import argparse
# hostname y fqdn
import socket
# existencia de archivos y validacion Cores.
import os
# Fecha y hora
from time import strftime

"""
  Clase para validar los diferentes tipos de datos recibidos y utilizados
  por la clase Install.
"""
class VerificaTipo(object):
   # Verifica si var es booleano
   def checkBool(self, var):
    return type(var) is bool

   # Verifica si var es entero.
   def checkInt(self,var):
    return type(var) is int

   # Verifica si var es una cadena que representa entero.
   def checkIntStr(self,var):
    ret=True
    if self.checkString(var):
      try:
        int(var)
      except ValueError:
        ret=False
    elif not type(var) is int:
      ret=False
    return ret

   # Verifica si var es cadena no vacia
   def checkString(self,var):
    ret=True
    if type(var) is str:
      if len(var)<1:
        ret=False
    else:
      ret=False
    return ret

   # Verifica si var es una cadena y cumple la estructura int.int.int.int
   def checkIpv4(self,var,full=None):
    ret=True
    if self.checkString(var):
     lst=var.split(".")
     c=var.count(".")
     lenLst=len(lst)
     # Verificar si tiene forma de IP v4
     if(c>0 and c<=3) or (lenLst>1 and lenLst<=4):
      for o in lst:
        if(not self.checkInt(o) and not self.checkIntStr(o) and o!="*"):
          ret=False
        elif(self.checkInt(o) and (o<0 or o>255)):
          ret=False
        elif(self.checkIntStr(o) and (int(o)<0 or int(o)>255)):
          ret=False
      # Si se solicita IP completa y no lo es, error.
      if(full and ret and var.find("*")>=0):
        ret=False
     else:
      ret=False
    else:
     ret=False
    return ret

   # Verifica si var es una nombre de usuario y cumple la estructura texto@dominio
   def checkUser(self, var):
    ret=True
    if var is None:
     ret=False
    if self.checkString(var):
     lst=var.split("@")  # dividir el nombre por la @
     if var.find("@")<0 or len(lst)<2:  # Si no hay arroba o no hay nada despues error.
      ret=False
     else:  # Si hay arroba, verificar que es un dominio.
      return self.checkDomain(lst[1])
    else:
     ret=False
    return ret

   # Verifica si var es una cadena y cumple la estructura texto.texto
   def checkDomain(self, var):
    ret=True
    if self.checkString(var):
     lst=var.split(".")
     if var.find(".")<0 or len(lst)<2:
      ret=False
    else:
     ret=False
    return ret

   # Verifica si var es una cadena y cumple la estructura text.texto.texto
   def checkFqdn(self, var):
    ret=True
    if self.checkString(var):
     lst=var.split(".")
     # Debe tener puntos y al menos 3 partes.
     if var.find(".")<0 or len(lst)<3:
      ret=False
    else:
     ret=False
    return ret

   # Verifica si var es una cadena, cumple la forma texto o text/texto
   #  y existe en el sistema de archivos.
   def checkFile(self,var):
    ret=True
    if self.checkString(var):
      if os.path.exists(var):
        if not os.path.isfile(var):
          ret=False
      else:
        try:  # El archivo no existe, tratar de crearlo.
          with open(var,"w"):
            pass
        except IOError as e:
          ret=False
    return ret

   # Verifica si var es un archivo con al menos minSize bytes.
   def checkFileSize(self,var,minSize):
    ret=True
    fileSize=0
    if self.checkFile(var):
     fileSize=os.path.getsize(var)
     if fileSize<minSize:
      ret=False
    return ret

   # Verifica si var es una cadena y cumple la forma texto o text/texto
   def checkPath(self,var):
    ret=True
    if self.checkString(var):
      lst=var.split("/")
      if var.find("/")<0 or len(lst)<2:
        ret=False
    else:
      ret=False
    return ret

   # Verifica si var es una cadena, cumple la forma texto o text/texto y existe
   #  en el sistema de archivos.
   def checkPathFile(self,var):
    ret=True
    if self.checkPath(var):
      if not self.checkFile(var):
        ret=False
    else:
      ret=False
    return(ret)

   """
   Detects the number of CPUs on a system. Cribbed from pp.
   http://codeliberates.blogspot.com.co/2008/05/detecting-cpuscores-in-python.html
   """
   def detectCPUs(self):
     # Linux, Unix and MacOS:
     if hasattr(os, "sysconf"):
       if "SC_NPROCESSORS_ONLN" in os.sysconf_names:
         # Linux & Unix:
         ncpus = os.sysconf("SC_NPROCESSORS_ONLN")
         if isinstance(ncpus, int) and ncpus > 0:
           return ncpus
       else:  # OSX:
         return int(os.popen2("sysctl -n hw.ncpu")[1].read())
      # Windows:
     if "NUMBER_OF_PROCESSORS" in os.environ:
       ncpus = int(os.environ["NUMBER_OF_PROCESSORS"])
       if ncpus > 0:
         return ncpus
     return(1)  # Default

   """
    Buscar searchStr en inputFile, si task es c, no se realiza busqueda,
    porque implica que el contenido del archivo será sobreescrito
    totalmente.
   """
   def findStrFile(self,task,inputFile,searchStr):
    ret=False
    if(task=="c"):
      return(ret)
    else:
      with open(inputFile,"r") as f:
        for l in f:
          if(searchStr in l):
            ret=True
      return(ret)

   def findStrConfig(self,config,searchStr):
    ret=False
    if(config.find(searchStr)!=-1):
      ret=True
    return(ret)

"""
  Clase encargada de procesar los argumentos y realizar la configuracion o
  reconfiguracion de HTCondor en el equipo actual.
"""
class Install(object):
  # Constructor, crea los mensajes y recolecta informacion
  def __init__(self,args,name):
     # Lista para almacenar los errores encontrados
     self.errores=[]
     # Lista para almacenar el orden de las opciones
     self.cfg_order=[]
     # Almacenar contenido a guardar en archivo de configuracion
     # self.configFile=""
     # Almacenar configuracion.
     self.config={}
     # Almacenar nombre del programa
     self.name=name
     # Copiar argumentos a variable del objeto.
     self.args=args
     # Obtener hostname
     self.hostname=socket.gethostname().lower()
     # Obtener fullhostname
     self.fqdn=socket.getfqdn().lower()
     # Obtener dominio basandose en el FQDN del equipo
     self.domain=".".join(self.fqdn.split(".")[1::])
     # print self.domain
     # Fecha y hora en que se ejecuta instalador, usando time
     self.hoy=strftime("%d/%m/%Y %H:%M:%S")
     # Datos de configuracion.
     self.configData="##### VALORES AGREGADOS POR %s el dia: %s #####" % (self.name,self.hoy)

     # Mensajes de error
     self.msgs_error={
       "err_task":"Task not defined (c/r) / Tarea no definida (c/r) ",
       "err_nodetype":"Missing node type (m,s,e,ms) / Tipo de nodo faltante (m,s,e,ms)",
       "err_config":"-cf: Invalid config file / Archivo de configuracion incorrecto",
       "err_nomaster":"-cm: Missing Master node's FQDN / Falta FQDN del nodo Maestro",
       "err_master":"-cm: Incorrect domain name for master / Nombre de dominio incorrecto para master",
       "err_wrongdomain":"Incorrect or missing domain name in computer configuration or arguments / Nombre de dominio incorrecto o faltante en la configuracion del equipo o argumentos",
       "err_domains":"-ed: Invalid extra domains / Dominios extra no validos",
       "err_ip":"-ip: Invalid IP address / Direccion IP no valida",
       "err_port":"-sp: Invalid Port number / Puerto no valido",
       "err_maxcpu":"-rs: Too many required CPUs / Demasiados procesadores (CPUs) requeridos",
       "err_maxmem":"-rs: Too many RAM required / Demasiada memoria (RAM) requerida",
       "err_masterslot":"-rs,-ds: Slots can't be created in Master or submit nodes / No se pueden crear slots en nodos maestro o de envio",
       "err_natip":"-nat: Invalid IP addresses / Direcciones IP no validas",
       "err_wrongowner":"-ou: Invalid owner\'s username / Nombre de propietario invalido",
       "err_nofile":"File not found / Archivo no encontrado"}
     # Verificar argumentos recibidos
     # self.checkArgs(args)

  # Metodo que convierte config en lineas de configuracion segun cfg_order.
  def config2Data(self,cfg_order,config):
    for item in cfg_order:
      try:
        # Si es lista de ClassAd,Valor y Comentario
        if(len(config[item])==3):
          comm=""
          # Si el comentario es de mas de 80 caracteres
          # partirlo en 2 lineas.
          if(len(config[item][2])>70):
            commLst=config[item][2].split("/")
            comm="%s\n#%s" % (commLst[0],commLst[1])
          else:
            comm=config[item][2]
          self.configData+="\n# %s\n%s = %s\n" % (comm,config[item][0],config[item][1])
        # Si es lista de ClassAd y Valor
        elif(len(config[item])==2):
          self.configData+="%s = %s\n" % (config[item][0],config[item][1])
        # Si es lista de contenido
        else:
          # Quitar los espacios sobrantes de cada linea.
          for l in config[item][0].split("\n"):
            self.configData+="%s\n" % (l.lstrip())
      except KeyError:  # si no esta la llave solicitada, no mostrar nada.
        pass

  # Metodo que evalua si se puede o no continuar la ejecucion
  def checkErrors(self):
     if len(self.errores)>0:
       print("----- ERRORS/ERRORES -----")
       self.showErrors()
       return False
     return True

  # Verificar archivo de configuración.
  def cfgConfigFile(self,valida):
    args=self.args
    ret=True
    # valida=VerificaTipo()
    # Validar que existe el archivo de configuración y que tiene datos.
    if args.task=='r' and not valida.checkFileSize(args.config,0):
      ret=False
      self.errores.append("err_config")
    # Validar que el archivo de configuracion es correcto.
    elif args.config and valida.checkString(args.config):
      # Convertir la ruta en una lista invertida para poder
      # acceder al nombre de archivo facilmente
      pathFile=args.config.split("/")[::-1]
      if not valida.checkPathFile(args.config):
        self.errores.append("err_config")
        ret=False
      elif not pathFile[0]=="condor_config.local":
        self.errores.append("err_configname")
        ret=False
    return ret

  # Verificar la tarea indicada y prepara configuracion inicial
  def cfgBegin(self,valida):
    args=self.args
    ret=True
    cfg_order=[]
    config={}
    # Si es configuracion se requieren tipo de nodo y si no es master, el master.
    if(args.task=="c"):
      if(not args.node):
        self.errores.append("err_nodetype")
        ret=False
      elif((args.node!="m" and args.node!="ms") and not args.master):
        self.errores.append("err_nomaster")
        ret=False
        return(ret)
      # Si es configuracion se definen estas classads.
      if(args.domain):
        config["cfg_admin"]=["CONDOR_ADMIN","root@$(FULL_HOSTNAME)","Contact's email / email de contacto"]
        config["cfg_uid"]=["UID_DOMAIN","%s" % args.domain,"User ID Domain"]
        config["cfg_fs"]=["FILESYSTEM_DOMAIN","%s" % args.domain,"Filesystem Domain"]
      elif(args.masterdomain):
        config["cfg_admin"]=["CONDOR_ADMIN","root@$(FULL_HOSTNAME)","Contact's email / email de contacto"]
        config["cfg_uid"]=["UID_DOMAIN","%s" % args.masterdomain,"User ID Domain"]
        config["cfg_fs"]=["FILESYSTEM_DOMAIN","%s" % args.masterdomain,"Filesystem Domain"]
      elif(self.domain):
        config["cfg_admin"]=["CONDOR_ADMIN","root@$(FULL_HOSTNAME)","Contact's email / email de contacto"]
        config["cfg_uid"]=["UID_DOMAIN","%s" % self.domain,"User ID Domain"]
        config["cfg_fs"]=["FILESYSTEM_DOMAIN","%s" % self.domain,"Filesystem Domain"]

    # Nodo maestro
    if(args.node=="m" or args.node=="ms"):
      # define rank to use local nodes first and remotes last.
      config["cfg_rank"]=["NEGOTIATOR_PRE_JOB_RANK","(IsRemote =!= True && isUndefined(RemoteOwner)) + isUndefined(RemoteOwner)","Use local and free nodes 1st / Usar nodos locales y libres primero"]
      if(args.node=="m"):
        config["cfg_type"]=["DAEMON_LIST","MASTER,COLLECTOR,NEGOTIATOR","Type: Condor Master"]
      elif(args.node=="ms"):
        config["cfg_type"]=["DAEMON_LIST","MASTER,COLLECTOR,NEGOTIATOR,SCHEDD","Type: Condor Master & Schedd"]
      if(args.usesp):  # Se solicito shared_port, anexar el servicio.
        config["cfg_type"][1]="%s,SHARED_PORT" % config["cfg_type"][1]
      # Se indico un dominio diferente.
      if(args.domain and valida.checkDomain(args.domain)):
        config["cfg_master"]=["CONDOR_HOST","$(FULL_HOSTNAME)","Condor Master"]
      # No se indico dominio, usar el FQDN del equipo.
      elif(args.task=="c" and valida.checkFqdn(self.fqdn)):
        config["cfg_master"]=["CONDOR_HOST","$(FULL_HOSTNAME)","Condor Master"]
      else:
        self.errores.append("err_wrongdomain")
        ret=False
    # Nodos no maestro.
    else:
      if(args.node=="s"):  # Submit node
        config["cfg_type"]=["DAEMON_LIST","MASTER,SCHEDD","Type: Condor Scheduller"]
      elif(args.node=="e"):  # Execute node
        config["cfg_type"]=["DAEMON_LIST","MASTER,STARTD","Type: Condor Worker"]
      # No se indico tipo de nodo y es configuracion.
      elif(args.task=="c"):
        self.errores.append("err_nodetype")
        ret=False

      # Se indico tipo de nodo y shared_port, anexar el servicio.
      if(args.usesp and args.node):
        config["cfg_type"][1]="%s,SHARED_PORT" % config["cfg_type"][1]
      # Verificar que el master indicado si es un FQDN.
      if(args.master and valida.checkFqdn(args.master)):
        config["cfg_master"]=["CONDOR_HOST","%s" % args.master,"Condor Master"]
        if(not args.domain or (args.domain and not valida.checkDomain(args.domain))):
          args.domain=args.masterdomain
      # Si es configuracion debe haber master,
      #  si es reconfig y se indico tipo de nodo, debe haber un master.
      elif(args.task=="c" or args.node):
        self.errores.append("err_nomaster")
        ret=False

    if(args.swap):  # Deshabilitar swap.
      config["cfg_swap"]=["RESERVED_SWAP","0","Deshabilitar uso de Swap / Disable Swap use."]

    if(ret):
      cfg_order.append("cfg_master")
      cfg_order.append("cfg_type")
      cfg_order.append("cfg_admin")
      cfg_order.append("cfg_uid")
      cfg_order.append("cfg_fs")
      cfg_order.append("cfg_swap")
      cfg_order.append("cfg_rank")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)
      # Actualizar argumentos
      self.args=args

  # Crea el allow_write.
  def cfgAllow(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se indico un master
    if(args.master):
      ret=True
      # Se indico un master y un dominio diferente al del master
      if(args.masterdomain!=args.domain and valida.checkDomain(args.domain)):
        config["cfg_write"]=["ALLOW_WRITE","*.%s,*.%s" % (args.domain,args.masterdomain),"Allowed computers / Equipos permitidos"]
      else:
        config["cfg_write"]=["ALLOW_WRITE","*.%s" % (args.masterdomain),"Allowed computers / Equipos permitidos"]
    # No se indico master pero si un dominio.
    elif(args.domain and valida.checkDomain(args.domain)):
      ret=True
      config["cfg_write"]=["ALLOW_WRITE","*.%s" % (args.domain),"Allowed computers / Equipos permitidos"]
    # No se indico master ni dominios, pero el equipo tiene dominio valido.
    elif(args.task=="c" and valida.checkDomain(self.domain)):
      ret=True
      config["cfg_write"]=["ALLOW_WRITE","*.%s" % (self.domain),"Allowed computers / Equipos permitidos"]
    # No se indicaron master, dominio o dominios.
    elif(args.task=="c"):
      self.errores.append("err_wrongdomain")
      ret=False
    # No se indico master ni dominio, pero si dominios.
    if(args.domains):
      dominios=[]
      doms=args.domains.split(",")
      for dom in doms:
        if(valida.checkIpv4(dom)):
          dominios.append("%s" % dom)
        elif(valida.checkDomain(dom)):
          dominios.append("*.%s" % dom)
      # Hay dominios y se indico master o dominio tambien.
      if(len(dominios)>0 and ("cfg_write" in config and len(config["cfg_write"])>1)):
        ret=True
        config["cfg_write"][1]="%s,%s" % (config["cfg_write"][1],",".join(dominios))
      # Solo se indicaron dominios.
      elif(len(dominios)>0):
        ret=True
        config["cfg_write"]=["ALLOW_WRITE","%s" % (",".join(dominios)),"Allowed computers / Equipos permitidos"]
        config["cfg_allow"]=["""
          # Allow connetions from ALLOW_WRITE domains/PCs.
          # Permitir conexiones desde los domininio y PCs en ALLOW_WRITE
          UPDATE_STARTD_AD=$(ALLOW_WRITE)
          #UPDATE_SCHEDD_AD=$(ALLOW_WRITE)
          ALLOW_ADVERTISE_MASTER = $(ALLOW_WRITE)
          ALLOW_ADVERTISE_STARTD = $(ALLOW_WRITE)
          #ALLOW_ADVERTISE_SCHEDD = $(ALLOW_WRITE)
         """]
      else:
        self.errores.append("err_domains")
        ret=False

    if(args.task=="r" and ret):
      config["cfg_write"][1]="$(ALLOW_WRITE),%s" % config["cfg_write"][1]
      config["cfg_allow"]=["""
        # Allow connetions from ALLOW_WRITE domains/PCs.
        # Permitir conexiones desde los domininio y PCs en ALLOW_WRITE
        UPDATE_STARTD_AD=$(ALLOW_WRITE)
        #UPDATE_SCHEDD_AD=$(ALLOW_WRITE)
        ALLOW_ADVERTISE_MASTER = $(ALLOW_WRITE)
        ALLOW_ADVERTISE_STARTD = $(ALLOW_WRITE)
        #ALLOW_ADVERTISE_SCHEDD = $(ALLOW_WRITE)
      """]
    if(ret):
      cfg_order.append("cfg_write")
      cfg_order.append("cfg_allow")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar conectividad tras NAT.
  def cfgNat(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se indico que hay NAT
    if(args.nodeips):
      # Validar que ambas IPs estan  completas.
      if(valida.checkIpv4(args.nodeips[0],True) and valida.checkIpv4(args.nodeips[1],True)):
        ret=True
        config["cfg_nat"]=["""
        # Node's IP outside NAT/IP del nodo fuera del NAT
        TCP_FORWARDING_HOST=%s
        # Node IP inside NAT/IP del nodo en el NAT
        PRIVATE_NETWORK_INTERFACE=%s
        # NAT's domain/Dominio del NAT"
        PRIVATE_NETWORK_NAME=$(UID_DOMAIN)""" % (args.nodeips[0],args.nodeips[1])]
      else:
        self.errores.append("err_natip")
        ret=False

    if(ret and args.nodeips):
      cfg_order.append("cfg_nat")
      """for idx in range(1,4):
        cfg_order.append("cfg_nat%s" % idx)"""
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de uso de IP.
  def cfgIp(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se indico un master
    if(args.ip):
      if(valida.checkIpv4(args.ip,True)):  # Validar que es una IP completa.
        ret=True
        config["cfg_ip"]=["NETWORK_INTERFACE","%s" % args.ip,"IP to use/IP a usar"]
      else:
        self.errores.append("err_ip")
        ret=False

    if(ret and args.ip):
      cfg_order.append("cfg_ip")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de uso de puertos.
  def cfgSharePort(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se solicito habilitar SharedPort
    if(args.usesp):
      ret=True
      config["cfg_port1"]=["USE_SHARED_PORT","True","Enable use a Shared port / Habilitar uso de un Shared Port"]

    # Se solicito habilitar SharedPort
    if(args.sport and valida.checkInt(args.sport)):
      ret=True
      if(args.usesp):  # Se indico usar shared_port
        config["cfg_port2"]=["SHARED_PORT_ARGS","-p %s" % args.sport,"Processes different than Collector use port %s/ Procesos diferentes al Collector usar puerto %s" %(args.sport,args.sport)]
      else:  # No se indico usar shared_port
        args.usesp=True
        config["cfg_port1"]=["USE_SHARED_PORT","True","Enable use a Shared port / Habilitar uso de un Shared Port"]
        config["cfg_port2"]=["SHARED_PORT_ARGS","-p %s" % args.sport,"Processes different to Collector use port %s/ Procesos diferentes a Collector usar puerto %s" %(args.sport,args.sport)]
    elif(args.sport and not valida.checkInt(args.sport)):
      self.errores.append("err_port")
      ret=False

    if(ret and args.usesp):
      cfg_order.append("cfg_port1")
      cfg_order.append("cfg_port2")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar ejecucion de tareas de usuarios no existentes
  def cfgTcp(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se solicito habilitar SharedPort
    if(args.usetcp):
      ret=True
      config["cfg_tcp"]=["UPDATE_COLLECTOR_WITH_TCP","True","Use TCP to Collector connections / Usar TCP para conexion con el Collector."]

    if(ret and args.usetcp):
      cfg_order.append("cfg_tcp")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de uso de recursos.
  def cfgSlots(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    slots=0
    if(args.task=="c" and args.node!="e" and (args.rs or args.ds)):
      self.errores.append("err_masterslot")
      ret=False
    # Se indico crear un slot para el usuario.
    else:
      if(args.rs):
        # Validar que no se pidieron todos los cores existentes.
        if(args.rs[0]<valida.detectCPUs()):
          if(args.rs[1]<71):
            # Hacer que se cree un slot dinamico con los recursos restantes.
            args.ds=True
            ret=True
            slots+=1
            config["cfg_rs"]=["""
            # Slots Configuration / Configuracion de Slots
            # Owner Slot / Slot para el propietario
            # Slot resources / Recursos del Slot
            SLOT_TYPE_%s = cpu=%s, ram=%s%%
            # Create Slot / Crear Slot
            NUM_SLOTS_TYPE_%s = 1
            # Never run jobs in this slot / Nunca ejecutar tareas en este slot
            SLOT_TYPE_%s_START=False""" % (slots,args.rs[0],args.rs[1],slots,slots)]
          else:
            args.rs=False
            self.errores.append("err_maxmem")
            ret=False
        else:
          args.rs=False
          self.errores.append("err_maxcpu")
          ret=False
      # Se solicito crear un slot dinamico.
      if(args.ds):
        ret=True
        slots+=1
        # Verificar si ya se creo ClassAd para exceso de RAM
        if(valida.findStrFile(args.task,args.config,"DISK_EXCEEDED") or valida.findStrConfig(self.configData,"DISK_EXCEEDED")):
          strHold="$(MEMORY_EXCEEDED) || $(DISK_EXCEEDED)"
          strReason="Job exceeded allowed resources. La tarea excedio los recursos permitidos."
        else:
          strHold="$(MEMORY_EXCEEDED)"
          strReason="Job exceeded available memory. La tarea excedio la memoria disponible."
        """
         Crear el slot con los recursos disponibles, es decir, si se
         solicitaron recursos para el usuario, usar solo lo que quedo,
         si no se solicito, usar todos los recursos.
        """
        config["cfg_ds"]=["""
        # Dynamic Slot / Slot Dinamico
        # Use only available resources for the Slot / usar solo los recursos disponibles para el Slot
        SLOT_TYPE_%s = cpu=auto, ram=auto
        # Enable dynamic resources in this Slot / Habilitar recursos dinamicos en este Slot
        SLOT_TYPE_%s_PARTITIONABLE = True
        # Create Slot / Crear Slot
        NUM_SLOTS_TYPE_%s = 1
        # Always run jobs in this slot / Siempre ejecutar tareas en este slot
        SLOT_TYPE_%s_START = True
        # Minimun Memory when job don't request any / Minimo de Memoria RAM cuando la tarea no solicita
        JOB_DEFAULT_REQUESTMEMORY=256
        MODIFY_REQUEST_EXPR_REQUESTMEMORY=quantize(RequestMemory, {256})
        # Check Memory used by the job / Verificar memoria usada por la tarea
        MEMORY_EXCEEDED=((MemoryUsage*1.1 > Memory) =?= TRUE)
        # If Memory Exceded, Evict job / Si se excede la memoria, cancelar la tarea
        PREEMPT=($(PREEMPT)) || $(MEMORY_EXCEEDED)
        WANT_SUSPEND=$(WANT_SUSPEND) && $(MEMORY_EXCEEDED)
        WANT_HOLD=%s
        # Reducir tiempo para borrar el slot de 10 a 2 minutos.
        MaxVacateTime = 2 * $(MINUTE)
        # Message to Job\'s owner / Mensaje para el propietario del Job.
        WANT_HOLD_REASON=ifThenElse( $(WANT_HOLD),\"%s\",undefined )""" % (slots,slots,slots,slots,strHold,strReason)]

      if(args.rs or args.ds):
        config["cfg_slots"]=["NUM_SLOTS","%s" % slots,"Create required Slots / Crear Slots requeridos"]
        # Registrar cantidad de slots para futuras referencias
        self.args.slots=slots

    if(ret):
      cfg_order.append("cfg_slots")
      if(args.rs):
        cfg_order.append("cfg_rs")
      if(args.ds):
        cfg_order.append("cfg_ds")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de uso de recursos.
  def cfgJobSize(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    if(args.task=="c" and args.node!="e" and args.ajs):
      self.errores.append("err_masterslot")
      ret=False
    # Se indico crear un slot para el usuario.
    else:
      # Se solicito crear un slot dinamico.
      if(args.ajs):
        ret=True
        strHold=""
        # Verificar si ya se creo ClassAd para exceso de RAM
        if(valida.findStrFile(args.task,args.config,"MEMORY_EXCEEDED") or valida.findStrConfig(self.configData,"MEMORY_EXCEEDED")):
          strHold="$(MEMORY_EXCEEDED) || $(DISK_EXCEEDED)"
          strReason="Job exceeded allowed resources. La tarea excedio los recursos permitidos."
        else:
          strHold="$(DISK_EXCEEDED)"
          strReason="Job exceeded allowed disk space. La tarea excedio el espacio en disco permitido."
        """
         Crear ClassAds para limitar uso en disco de las tareas.
        """
        config["cfg_ajs"]=["""
        # Uncomment for Debug / Desomente para depuracion
        #STARTD_DEBUG = D_FULLDEBUG
        # Define maximum space to use for a Job.
        # Definir maximo espacio a usar por una tarea (%s MB)
        JOB_DEFAULT_REQUESTDISK=%s
        MODIFY_REQUEST_EXPR_REQUESTDISK=quantize(RequestDisk, {%s})
        # Check Disk if disk space used by the job is greater than slot disk.
        # Verificar si el espacio en disco usado por la tarea es mayor que el del slot.
        DISK_EXCEEDED = DiskUsage > MY.TotalSlotDisk
        PREEMPT = ($(PREEMPT)) || ($(DISK_EXCEEDED))
        WANT_SUSPEND=$(WANT_SUSPEND) && $(DISK_EXCEEDED)
        WANT_HOLD=%s
        # Reducir tiempo para borrar el slot de 10 a 2 minutos.
        MaxVacateTime = 2 * $(MINUTE)
        # Message to Job's owner / Mensaje para el propietario del Job.
        WANT_HOLD_REASON=ifThenElse( $(WANT_HOLD),\"%s\",undefined )""" % (args.ajs,args.ajs * 1024,args.ajs * 1024,strHold,strReason)]
        # Si se crearon slots antes
        # Se solicitaron recursos para el propietario.
        if(valida.findStrFile(args.task,args.config,"SLOT_TYPE_2_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_2_START = True")):
          config["cfg_ajs_start"]=["SLOT_TYPE_2_START","$(SLOT_TYPE_2_START) && IfThenElse(isUndefined(TARGET.JobSize),TRUE, TARGET.JobSize < %s)" % ((args.ajs / 2) * 1024),"Maximum job size accepted / Tamaño maximo de tarea aceptado."]
        # No se solicito reservar recursos para el propietario.
        elif(valida.findStrFile(args.task,args.config,"SLOT_TYPE_1_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_1_START = True")):
            config["cfg_ajs_start"]=["SLOT_TYPE_1_START","$(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.JobSize),True, TARGET.JobSize < %s)" % ((args.ajs / 2) * 1024),"Maximum job size accepted / Tamaño maximo de tarea aceptado."]
        # No se crearon slots antes
        else:
          config["cfg_ajs_start"]=["START","$(START) && IfThenElse(isUndefined(TARGET.JobSize),TRUE, TARGET.JobSize < %s)" % ((args.ajs / 2) * 1024),"Maximum job size accepted / Tamaño maximo de tarea aceptado."]
    if(ret):
      cfg_order.append("cfg_ajs")
      cfg_order.append("cfg_ajs_start")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de Prioridad de Usuario
  def cfgUserPrio(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se desea restringuir por prioridad.
    if(args.userprio):
      if(valida.checkInt(args.userprio)):
        if(args.userprio<600):
          args.userprio=600
        ret=True
        """
        # Restricion de prioridad.
        SLOT_TYPE_1_START = $(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.SubmitterUserPrio),True, TARGET.SubmitterUserPrio < 505.0)
        """
        if(valida.findStrFile(args.task,args.config,"SLOT_TYPE_2_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_2_START = True")):
          # Ejecutar solo tareas del propietario.
          config["cfg_usrprio"]=["SLOT_TYPE_2_START","$(SLOT_TYPE_2_START) && IfThenElse(isUndefined(TARGET.SubmitterUserPrio),True, TARGET.SubmitterUserPrio < %s.0)" % args.userprio,"Restriction for users with high use of resources / Restriccion para usuarios con alto uso de recursos"]
        elif(valida.findStrFile(args.task,args.config,"SLOT_TYPE_1_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_1_START = True")):
          config["cfg_usrprio"]=["SLOT_TYPE_1_START","$(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.SubmitterUserPrio),True, TARGET.SubmitterUserPrio < %s.0)" % args.userprio,"Restriction for users with high use of resources / Restriccion para usuarios con alto uso de recursos"]
        else:
          config["cfg_usrprio"]=["STARTD_ATTRS","$(STARTD_ATTRS) && IfThenElse(isUndefined(TARGET.SubmitterUserPrio),True, TARGET.SubmitterUserPrio < %s.0)" % args.userprio,"Restriction for users with high use of resources / Restriccion para usuarios con alto uso de recursos"]
      else:
        self.errores.append("err_wrongprio")
        ret=False
    if(ret and args.userprio):
      cfg_order.append("cfg_usrprio")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de Slots para el Usuario
  def cfgUserSlots(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se desea restringuir por prioridad.
    if(args.userslots):
      if(valida.checkInt(args.userslots)):
        if(args.userslots<1):
          args.userslots=1
        ret=True
        """
        # Restricion de slots. Cada usuario puede usar solo 1 slot.
        #SLOT_TYPE_1_START = $(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.SubmitterUserResourcesInUse),True, TARGET.SubmitterUserResourcesInUse < 1)
        """
        if(valida.findStrFile(args.task,args.config,"SLOT_TYPE_2_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_2_START = True")):
          # Ejecutar solo tareas del propietario.
          config["cfg_usrslots"]=["SLOT_TYPE_2_START","$(SLOT_TYPE_2_START) && IfThenElse(isUndefined(TARGET.SubmitterUserResourcesInUse),True, TARGET.SubmitterUserResourcesInUse < %s)" % args.userslots,"Restriction for users with high use of resources / Restriccion para usuarios con alto uso de recursos"]
        elif(valida.findStrFile(args.task,args.config,"SLOT_TYPE_1_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_1_START = True")):
          config["cfg_usrslots"]=["SLOT_TYPE_1_START","$(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.SubmitterUserResourcesInUse),True, TARGET.SubmitterUserResourcesInUse < %s)" % args.userslots,"Restriction for users with high use of resources / Restriccion para usuarios con alto uso de recursos"]
        else:
          config["cfg_usrslots"]=["STARTD_ATTRS","$(STARTD_ATTRS) && IfThenElse(isUndefined(TARGET.SubmitterUserResourcesInUse),True, TARGET.SubmitterUserResourcesInUse < %s)" % args.userslots,"Restriction for users with high use of resources / Restriccion para usuarios con alto uso de recursos"]
      else:
        self.errores.append("err_wrongslots")
        ret=False
    if(ret and args.userslots):
      cfg_order.append("cfg_usrslots")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar restriccion de Reinicios por Tarea
  def cfgJobStart(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se desea restringuir por prioridad.
    if(args.jobstart):
      if(valida.checkInt(args.jobstart)):
        if(args.jobstart<2):
          args.jobstart=2
        ret=True
        """
        # Restricion de slots. Cada usuario puede usar solo 1 slot.
        #SLOT_TYPE_1_START = $(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.SubmitterUserResourcesInUse),True, TARGET.SubmitterUserResourcesInUse < 1)
        """
        if(valida.findStrFile(args.task,args.config,"SLOT_TYPE_2_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_2_START = True")):
          # Ejecutar solo tareas del propietario.
          config["cfg_jobstart"]=["SLOT_TYPE_2_START","$(SLOT_TYPE_2_START) && IfThenElse(isUndefined(TARGET.NumJobStarts),True, TARGET.NumJobStarts < %s)" % args.jobstart,"Restriction for Jobs with multiple failures / Restriccion para Tareas con multiples fallos"]
        elif(valida.findStrFile(args.task,args.config,"SLOT_TYPE_1_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_1_START = True")):
          config["cfg_jobstart"]=["SLOT_TYPE_1_START","$(SLOT_TYPE_1_START) && IfThenElse(isUndefined(TARGET.NumJobStarts),True, TARGET.NumJobStarts < %s)" % args.jobstart,"Restriction for Jobs with multiple failures / Restriccion para Tareas con multiples fallos"]
        else:
          config["cfg_jobstart"]=["STARTD_ATTRS","$(STARTD_ATTRS) && IfThenElse(isUndefined(TARGET.NumJobStarts),True, TARGET.NumJobStarts < %s)" % args.jobstart,"Restriction for Jobs with multiple failures / Restriccion para Tareas con multiples fallos"]
      else:
        self.errores.append("err_wrongstarts")
        ret=False
    if(ret and args.jobstart):
      cfg_order.append("cfg_jobstart")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar ejecucion de tareas de usuarios no existentes
  def cfgNoUser(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se solicito habilitar SharedPort
    if(args.nu):
      ret=True
      config["cfg_nu1"]=["SHADOW_RUN_UNKNOWN_USER_JOBS","True","Enable unexistent user jobs / Permitir tareas de usuarios no existentes"]
      config["cfg_nu2"]=["SOFT_UID_DOMAIN","True"]

    if(ret and args.nu):
      cfg_order.append("cfg_nu1")
      cfg_order.append("cfg_nu2")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Definir usuario propietario del nodo y darle prioridad a sus tareas.
  def cfgOwner(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se indico un propietario.
    if(args.owneruser):
      if(valida.checkUser(args.owneruser[0])):
        ret=True
        config["cfg_owner1"]=["MachineOwner","\"%s\"" % args.owneruser[0],"CustomClassAd for Owner priority / ClassAd personal para prioridad del propietario"]
        config["cfg_owner2"]=["STARTD_ATTRS","$(STARTD_ATTRS) MachineOwner"]
        if(args.owneruser[1]=="S"):
          config["cfg_owner3"]=["RANK","User =?= MY.MachineOwner","Uncommented priorize owner jobs but accept jobs from any user / Descomentado priorizar tareas del propietario, pero aceptar de todos los usuarios."]
        else:
          if(valida.findStrFile(args.task,args.config,"SLOT_TYPE_2_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_2_START = True")):
            # Ejecutar solo tareas del propietario.
            config["cfg_owner3"]=["SLOT_TYPE_2_START","$(SLOT_TYPE_2_START) && TARGET.User == MY.MachineOwner","Only jobs from Owner are acepted / Solo las tareas del propietario son aceptadas."]
          elif(valida.findStrFile(args.task,args.config,"SLOT_TYPE_1_START = True") or valida.findStrConfig(self.configData,"SLOT_TYPE_1_START = True")):
            config["cfg_owner3"]=["SLOT_TYPE_1_START","$(SLOT_TYPE_1_START) && TARGET.User == MY.MachineOwner","Only jobs from Owner are acepted / Solo las tareas del propietario son aceptadas."]
          else:
            config["cfg_owner3"]=["START","$(START) && TARGET.User == MY.MachineOwner","Only jobs from Owner are acepted / Solo las tareas del propietario son aceptadas."]
      else:
        self.errores.append("err_wrongowner")
        ret=False
    if(ret and args.owneruser):
      for idx in range(1,4):
        cfg_order.append("cfg_owner%s" % idx)
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar clave para MasterSubmit
  def cfgPassMS(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Si el nodo es MasterSubmit y se solicito habilitar Clave
    if(args.passms):
      ret=True
      config["cfg_passms"]=["""
        # Enable Password security for the Pool.
        # Habilitar seguridad por clave para el pool.
        SEC_DEFAULT_AUTHENTICATION = OPTIONAL
        # Allow passwords
        SEC_DEFAULT_AUTHENTICATION_METHODS = PASSWORD, FS, $(SEC_DEFAULT_AUTHENTICATION_METHODS)
        ALLOW_DAEMON = condor_pool@*
        SEC_PASSWORD_FILE = /etc/condor/poolpass
        SEC_DAEMON_INTEGRITY = REQUIRED
        SEC_CLIENT_AUTHENTICATION_METHODS = PASSWORD, FS, $(SEC_CLIENT_AUTHENTICATION_METHODS)
       """]
    if(ret):
      cfg_order.append("cfg_passms")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar clave para Execute
  def cfgPassEX(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Si el nodo es de ejecucion y se solicito habilitar Clave
    if(args.passex):
      ret=True
      config["cfg_passex"]=["""
        # Enable Password security for the Pool.
        # Habilitar seguridad por clave para el pool.
        SEC_PASSWORD_FILE = /etc/condor/poolpass
        SEC_DAEMON_INTEGRITY = REQUIRED
        SEC_DAEMON_AUTHENTICATION = REQUIRED
        SEC_DAEMON_AUTHENTICATION_METHODS = PASSWORD, FS, $(SEC_DAEMON_AUTHENTICATION_METHODS)
        SEC_CLIENT_AUTHENTICATION_METHODS = PASSWORD, FS, $(SEC_CLIENT_AUTHENTICATION_METHODS)
        ALLOW_DAEMON = condor_pool@*
       """]

    if(ret):
      cfg_order.append("cfg_passex")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar envio de tareas MPI
  def cfgMpiSched(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Si el nodo puede enviar y se solicito habilitar MPI
    if(args.mpis and (args.node=="ms" or args.node=="s")):
      ret=True
      config["cfg_mpis1"]=["UNUSED_CLAIM_TIMEOUT","0","Allow MPI jobs to be sent/Permitir envio de tareas MPI."]
      config["cfg_mpis2"]=["MPI_CONDOR_RSH_PATH","$(LIBEXEC)"]
      config["cfg_mpis3"]=["ALTERNATE_STARTER_2","$(SBIN)/condor_starter"]
      config["cfg_mpis4"]=["STARTER_2_IS_DC","True"]
      config["cfg_mpis5"]=["SHADOW_MPI","$(SBIN)/condor_shadow"]

    if(ret):
      for idx in range(1,6):
        cfg_order.append("cfg_mpis%s" % idx)
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar ejecucion de tareas MPI
  def cfgMpiNode(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Si el nodo es de ejecucion y se solicito habilitar MPI
    if(args.mpin and args.node=="e"):
      ret=True
      config["cfg_mpin1"]=["DedicatedScheduler","\"DedicatedScheduler@%s\"" % args.master,"Allow MPI jobs to be run/Permitir ejecucion de tareas MPI."]
      config["cfg_mpin2"]=["START","$(START) || True"]
      config["cfg_mpin3"]=["STARTD_ATTRS","$(STARTD_ATTRS), DedicatedScheduler"]
      config["cfg_mpin4"]=["STARTD_EXPRS","$(STARTD_EXPRS), DedicatedScheduler"]
      config["cfg_mpin5"]=["SUSPEND","$(SUSPEND) || False"]
      config["cfg_mpin6"]=["CONTINUE","$(CONTINUE) || True"]
      config["cfg_mpin7"]=["PREEMPT","$(PREEMPT) || False"]
      config["cfg_mpin8"]=["KILL","$(KILL) || False"]
      config["cfg_mpin9"]=["WANT_SUSPEND","$(WANT_SUSPEND) || False"]
      config["cfg_mpin10"]=["WANT_VACATE","$(WANT_VACATE) || False"]
      config["cfg_mpin11"]=["RANK","$(RANK) && Scheduler =?= $(DedicatedScheduler)"]
      config["cfg_mpin12"]=["MPI_CONDOR_RSH_PATH","$(LIBEXEC)"]
      config["cfg_mpin13"]=["CONDOR_SSHD","/usr/sbin/sshd"]
      config["cfg_mpin14"]=["CONDOR_SSH_KEYGEN","/usr/bin/ssh-keygen"]

    if(ret):
      for idx in range(1,15):
        cfg_order.append("cfg_mpin%s" % idx)
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Habilitar universo Docker
  def cfgDocker(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Si el nodo es de ejecucion y se solicito habilitar MPI
    if(args.docker and args.node=="e"):
      ret=True
      config["cfg_docker"]=["DOCKER","/usr/bin/docker", "Path to Docker"]

    if(ret):
      cfg_order.append("cfg_docker")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Definir nodo como Remoto
  def cfgRemoteNode(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Si el nodo es de ejecucion y se solicito habilitar MPI
    if(args.rn and args.node=="e"):
      ret=True
      config["cfg_remote"]=["""
       # This node is outside Pool's LAN
       IsRemote = True
       STARTD_ATTRS = $(STARTD_ATTRS) && (Target.MayUseAWS || Target.MayUseGCP || Target.MayUseIBM) IsRemote
      """]
    """
    startExpression = "START = MayUseAWS == TRUE\n";
    startExpression,
    "START = (MayUseAWS == TRUE) && stringListMember( Owner, \"%s\" )\n"
    """
    if(ret):
      cfg_order.append("cfg_remote")
      # Crear contenido inicial
      self.config2Data(cfg_order,config)

  # Crear cronJobs.
  def cfgCronJob(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se indico que hay NAT
    if(args.cronjob):
      # Validar que el script indicado existe.
      if valida.checkPathFile(args.cronjob[1]):
        ret=True
        config["cfg_cronjob"]=["""
        # User's Cronjob
        STARTD_CRON_JOBLIST = $(STARTD_CRON_JOBLIST) %s
        STARTD_CRON_%s_PREFIX = MY_
        STARTD_CRON_%s_EXECUTABLE = %s
        STARTD_CRON_%s_PERIOD = %s
        STARTD_CRON_%s_MODE = periodic
        STARTD_CRON_%s_RECONFIG = false
        STARTD_CRON_%s_KILL = true
        STARTD_CRON_%s_ARGS = %s""" % (args.cronjob[0],args.cronjob[0],args.cronjob[0],\
       args.cronjob[1],args.cronjob[0],args.cronjob[2],\
       args.cronjob[0],args.cronjob[0],args.cronjob[0],\
       args.cronjob[0],args.cronjob[3])]
      else:
        self.errores.append("err_nofile")
        ret=False

    if(ret and args.cronjob):
      cfg_order.append("cfg_cronjob")
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Habilitar apagado del nodo si no hay tareas
  def cfgAutoShutdown(self,valida):
    args=self.args
    ret=False
    cfg_order=[]
    config={}
    # Se indico que hay NAT
    if(args.shutdown):
      # Verificar que el script de apago existe.
      if valida.checkPathFile("/etc/condor/shutdown.sh"):
        ret=True
        config["cfg_shutdown"]=["""
        # Tell HTCondor daemons to gracefully exit if the condor_startd observes
        # that it has had no active claims for more than 5 minutes and 30 seconds.
        STARTD_NOCLAIM_SHUTDOWN = 330

        # Next, tell the condor_master to run a script as root upon exit.
        # In our case, this script will shut down the node.
        DEFAULT_MASTER_SHUTDOWN_SCRIPT = /etc/condor/shutdown.sh

        # This final config knob is for the paranoid, and covers
        # the case that perhaps the condor_startd crashes.  It tells the
        # condor_master to exit if it notices for any reason that the
        # condor_startd is not running within 1 minute of startup.
        MASTER.DAEMON_SHUTDOWN_FAST = ( STARTD_StartTime == 0 ) && ((time() - DaemonStartTime) > 60)"""]
      else:
        self.errores.append("err_nofile")
        ret=False
    if(ret and args.shutdown):
      cfg_order.append("cfg_shutdown")
      """for idx in range(1,4):
        cfg_order.append("cfg_nat%s" % idx)"""
      # Crear contenido
      self.config2Data(cfg_order,config)

  # Crear configuracion y almacenarla en el archivo respectivo.
  def buildConfig(self):
    valida=VerificaTipo()
    # Si se indico master, extraer el dominio.
    if(self.args.master and valida.checkFqdn(self.args.master)):
      master_fqdn=self.args.master.split(".")[1:]
      self.args.masterdomain=".".join(master_fqdn)
    else:
      self.args.masterdomain=None
    # Verificar archivo de configuracion.
    self.cfgConfigFile(valida)
    # Iniciar configuracion.
    self.cfgBegin(valida)
    self.cfgAllow(valida)
    self.cfgNat(valida)
    self.cfgIp(valida)
    self.cfgSharePort(valida)
    self.cfgTcp(valida)
    self.cfgSlots(valida)
    self.cfgOwner(valida)
    self.cfgJobSize(valida)
    self.cfgUserPrio(valida)
    self.cfgUserSlots(valida)
    self.cfgJobStart(valida)
    self.cfgNoUser(valida)
    self.cfgPassMS(valida)
    self.cfgPassEX(valida)
    self.cfgMpiSched(valida)
    self.cfgMpiNode(valida)
    self.cfgDocker(valida)
    self.cfgRemoteNode(valida)
    self.cfgCronJob(valida)
    self.cfgAutoShutdown(valida)

    if(not self.checkErrors()):
     return False
    print(self.configData)
    # guardar datos
    if(self.args.task=="c"):
     with open(self.args.config, "wt") as configFile:
       configFile.write(self.configData)
    elif(self.args.task=="r"):
     with open(self.args.config, "at") as configFile:
       configFile.write("\n\n%s" % self.configData)

    # Si es nodo de envio, crear ejemplo
    if(self.args.node=="ms" or self.args.node=="s"):
      exampleSubmit="""
##
# Test example Submit File
##
# Use: condor_submit checkCondor.condor
should_transfer_files = Yes
when_to_transfer_output = ON_EXIT_OR_EVICT
transfer_input_files = test.bash
Executable = /bin/bash
Arguments  = test.bash 61
Universe   = vanilla
Log        = HostLog.txt
Output     = HostOut.$(Cluster)_$(Process).txt
Error      = HostErr.$(Cluster)_$(Process).txt
# restart jobs that are in Hold state and have run less than 10 times.
#Periodic_Release = ((JobStatus==5) && JobRunCount <= 10)
Queue 5
"""
      with open("checkCondor.condor", "wt") as exFile:
        exFile.write(exampleSubmit)
      exampleSubmit="""
#!/bin/bash
##
# test.bash
# script used for checkCondor.condor
##
# Use: condor_submit checkCondor.condor
d=$(date)
h=$(hostname -f)
echo "sleep: ${1} host: ${h} date: ${d}"
sleep $1
"""
      with open("test.bash", "wt") as exFile:
        exFile.write(exampleSubmit)

    """
    # Si se indica CCB, configurar
    #     if args.ip:
    #       self.config["cfg_ip"]=["NETWORK_INTERFACE","%s" % args.ip,"IP to use/IP a usar"]
    """

  # Muestra los errores encontrados.
  def showErrors(self):
     for err in self.errores:
      print("Error [%s]: %s" % (err,self.msgs_error[err]))

parser = argparse.ArgumentParser(
  formatter_class=argparse.ArgumentDefaultsHelpFormatter,
  description='=> HTCondor Configurer <=',
  epilog='Ex/Ej: python %(prog)s c -nt m -cf /etc/condor_config.local')

grp1=parser.add_argument_group('Required/Requerido')
grp1.add_argument('task', action="store", choices=['c', 'r'], help="Task type/Tipo de tarea: c=Configure/Configurar, r=Reconfigure/Reconfigurar")

grp2=parser.add_argument_group('Common/Comunes')
grp2.add_argument('-cf', '--config-file', action="store", dest="config", help="Path to condor_config.local/Ruta a condor_config.local")
grp2.add_argument('-nt', '--node-type', action="store", dest="node", choices=['m', 's', 'e', 'ms'], help="Node type/Tipo de nodo: m=Master, s=Submit, e=Execute, ms=Master Submit")
grp2.add_argument('-ns', '--no-swap', action="store_true", dest="swap", default=False, help="Don't use swap/No usar Swap.")
grp2.add_argument('-cm', '--condor-master', action="store", dest="master", help="Central Manager (FQDN).")

grp3=parser.add_argument_group('Network parameters/Parametros de red')
grp3.add_argument('-nd', '--network-domain', action="store", dest="domain", help="Network's domain/Dominio de red.")
grp3.add_argument('-ed', '--extra-domains', action="store", dest="domains", help="Domains allowed to sent jobs (Ex: *.domain1,*.domain2,192.168.*)/Dominios autorizados para enviar tareas (Ej: *.domain1,*.domain2,192.168.*).")
grp3.add_argument('-nat', '--nat-ips', action="store", dest="nodeips", nargs=2, help="Public and NAT IPs of the node. Ex -nat 8.8.1.4 192.168.1.2/IP publico y en NAT del nodo. Ej. -nat 8.8.1.4 192.168.1.2")
grp3.add_argument('-ip', '--ip-address', action="store", dest="ip", help="IP to use/IP a usar.")
grp3.add_argument('-usp', '--use-shared-port', action="store_true", dest="usesp", default=False, help="Make all process uses same port than Collector (9618)/Hacer que  todos los procesos usen el mismo puerto que el Collector (9618).")
grp3.add_argument('-sp', '--shared-port', action="store", dest="sport", type=int, help="Make all process except Collector to use only port SPORT/Hacer que todos los procesos excepto Collector usen el puerto SPORT.")
grp3.add_argument('-tcp', '--use-tcp', action="store_true", dest="usetcp", default=False, help="Use TCP for Collector connections/Usar TCP para conexiones con el Collector.")

grp4=parser.add_argument_group('User and resources\'s parameters/Parametros de Usuario y recursos')
grp4.add_argument('-nu', '--nobody-user', action="store_true", dest="nu", default=False, help="Enable tasks from users not created in the node/Permitir tareas de usuarios no existentes en el nodo.")
grp4.add_argument('-ou', '--owner-user', action="store", dest="owneruser", nargs=2, help="Full username of the node\'s owner and type of use ([P] private or [S] shared). Ex -ou johndoe@cloud.test.org S / Nombre de usuario completo del propietario del nodo y tipo de uso ([P] privado o [S] compartido). Ej. -ou johndoe@cloud.test.org S")
grp4.add_argument('-rs', '--reserved-slot', action="store", dest="rs", type=int, nargs=2, help="CPU and RAM for the user's reserved slot. Ex -rs 1 10 for 1 core and 10%% RAM/Cores y RAM para el slot dedicado al usuario. Ej. -rs 1 10 para 1 core y 10%% de RAM")
grp4.add_argument('-ds', '--dynamic-slot', action="store_true", dest="ds", default=False, help="Create an uniq and dynamic slot with all resources/Crear un slot unico y dinamico con todos los recursos.")
#grp4.add_argument('-pn', '--private-node', action="store_true", default=False, dest="privnode", help="Define this node as private, it means, only 'owner user' job's are accepted./Define este nodo como privado, es decir, solo las tareas del \'propietario\' son ejecutadas.")
grp4.add_argument('-ajs', '--accepted-jobsize', action="store", dest="ajs", type=int, help="Maximum Job running size allowed, the maximum accepted JobSize is half this value. Ex -ajs 100 accept jobs until 50MB and hold jobs than exceeds 100MB in disk/Máximo tamaño en disco permitido. Ej. -ajs 100 acepta tareas de hasta 50MB y detiene tareas que ocupen mas de 100MB en disco.")
grp4.add_argument('-aup', '--accepted-user-priority', action="store", dest="userprio", type=int, help="Maximun User priority allowed to run jobs in the node (must be greater than 600). Ex -aup 1000 / Prioridad de usuario máxima permitida para ejecutar tareas en el nodo (debe ser mayor a 600). Ej. -aup 1000.")
grp4.add_argument('-mus', '--maximun-user-slots', action="store", dest="userslots", type=int, help="Maximun Slots allowed to use for a user (must be greater than 0). Ex -mus 100 / Maximo de Slots permitidos para un usuario (debe ser mayor a 0). Ej. -mus 1.")
grp4.add_argument('-mjs', '--maximun-job-starts', action="store", dest="jobstart", type=int, help="Maximun limit of job restarts accepted (must be greater than 1). Ex -mjs 2 / Limite maximo de reinicios de tareas acceptado (debe ser mayor a 1). Ej. -mjs 2")

grp5=parser.add_argument_group('Security parameters/Parametros de Seguridad')
grp5.add_argument('-passms', '--password-ms', action="store_true", default=False, dest="passms", help="Enable password for MasterSubmit node, save password in /etc/condor/poolpass./Habilitar clave para nodo MasterEnvio, guardar clave en /etc/condor/poolpass.")
grp5.add_argument('-passex', '--password-ex', action="store_true", default=False, dest="passex", help="Enable password for Excecute node, save password in /etc/condor/poolpass./Habilitar clave para nodo de Ejecucion, guardar clave en /etc/condor/poolpass.")

grp6=parser.add_argument_group('Extra parameters/Parametros extra')
grp6.add_argument('-mpis', '--mpi-sched', action="store_true", default=False, dest="mpis", help="Allow MPI jobs to be sent/Permitir envio de tareas MPI.")
grp6.add_argument('-mpin', '--mpi-node', action="store_true", default=False, dest="mpin", help="Allow MPI jobs to be run/Permitir ejecucion de tareas MPI.")
grp6.add_argument('-docker', '--docker', action="store_true", default=False, dest="docker", help="Allow Docker universe tasks/Permitir tareas universo Docker.")
grp6.add_argument('-rn', '--remote-node', action="store_true", default=False, dest="rn", help="Define this node as Remote (not in the same LAN)/Define este nodo como Remoto (No en la misma LAN).")
grp6.add_argument('-cj', '--cron-job', action="store", dest="cronjob", nargs=4, help="Name, Script's pathname, periodicity and arguments for STARTD_CRON. Ex -cj mycron /etc/condor/cron.bash 15m \"myarg1=1 myarg2=2\" / Nombre, pathname del script, periodicidad y argumentos para STARD_CRON. Ej. -cj mycron /etc/condor/cron.bash 15m \"myarg1=1 myarg2=2\"")
grp6.add_argument('-as', '--auto-shutdown', action="store_true", default=False, dest="shutdown", help="Enable automatic shutdown if node iddle for more than 15 minutes. / Habilitar apagado automatico si el nodo esta libre por mas de 15 minutos.")

result=parser.parse_args()
# print(result)

if(not result.config):
  print("-cf: Invalid or missing config file / Archivo de configuracion incorrecto o faltante")
  exit(1)

# Se configurara una instalacion
print("Iniciando procesamiento / Starting processing")
ins=Install(result,"htconfig_v2.py")
ins.buildConfig()

# python htconfig.py c -cf ./condor_config.local -cm condor-headnode.univalle.edu.co -nd univalle.edu.co -ed *.eisc.univalle.edu.co,172.18.1.* -sp 9619 -nat 192.168.131.2 172.18.1.249 -nt e -ip 172.18.1.249 -mpin
