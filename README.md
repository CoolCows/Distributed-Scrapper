# Distributed-Scrapper
**Autores**: Rodrigo Pino, Adrian Portales, C-412

### Intro

### Chord

### Scraper Chord

Un nodo de este tipo es una especializacion de la clase Chord implementada explicitamente para utilizar los servicio de un scraper. El funcionamiento de este nodo esta compuesto por tres hilos principales, cada uno desempennando una funcion unica y de igual importancia:

* Comunicacion con cliente: Un hilo encargado de las comunicaciones con los clientes. Este toma todos los pedidos que le hagan los clientes. Si este pedidos se encarga dicho nodo chord lo busca en su cache, en caso de no encontrarlos pide el servicio de un scraper en la red.

* Comunicacion con scraper: Este hilo es el encargado de pedir cuando necesita responder a pedidos de comunicarse con el scraper. En caso de que tenga muchos pedidos pendientes buscara el apoyo de otros scrapers en la red.

* Recibidor de trabajos de scraper: Una vez el hilo encargado de comunicacion con el scraper obtenga conexion de un scraper. Estos empezaran a enviar trabajo a este hilo. Este hilo tiene sockets tipo push donde envia sus trabajos y sockets tipo pull donde los devuelve.

### Scraper

Este nodo ofrece el servicio de scrapear en la red. Un nodo le pide sus servicios y este responde si o no de acuerda a la cantidad de trabajadores que tenga el momento. En el caso de responder si, este crea un nuevo hilo trabajador que se conecta a cierto puerto especificado por el requester. Una vez conectados obtiene los trabajos a traves de hacer pull, y una vez finalizado empieza a hacer push.

### Chord Client

Este es un nodo especifico para comunicarse con nodos Chord Scraper: Los pedidos se envian a traves de este. Este nodo cuando se hace una busqueda en profundidad el solo se gestiona. Sabe que pedir para completar su trabajo.

