# Distributed-Scrapper
**Autores**: Rodrigo Pino, Adrian Portales, C-412

### Intro

Nuestro sistema esta compuesto por 3 tipos de nodos distintos, Scraper, Cliente y Chord. El cliente es el que se encarga de realizar los pedidos el nodo chord, funciona como cache y enrutador, si contiene el url lo envia, sino pide el servicio de algun scraper para que analize el request. Las ventajas de este sistema constituyen a mayor cantidad de nodos chord y scrapers en la red representan mayor poder de computo y mayor velocidad a la hora de realizar pedidos. Ademas como un sistema distribuido descentralizado ningun nodo es vital para el funcionamiento de la red, mientras exista un nodo chord y un scraper es posbile seguir haciendo request (Quizas con un poco de lag) El desafio en esto esta en manetener los nodos que que integran la red en "armonia", que el sistema no llegue a un punto muerto donde una funcion de un nodo deje de funcionar, como el encargado de recibir request, que a ojos de todos sigue funcionando, pero evidentemente no, luego los keys que le pertenezcan no podren obtenerse a no ser que se cierre el nodo manualmente y la red detecte un fallo en el sistema.

Mientras mas nodos en la red mas poder de computo. Hace falta hallar un balance entre nodos chord y scrapers. Un nodo chord en nuestros ensayos con una cola llena de pedidos no puede manter mas 5 hilos del scraper a la vez. Se les van cerrando. Luego muy pocos nodos chord y muchos scrapers no es bueno, xq no se utilizan todos. Lo contrario tampoco fitdoing xq muchos nodos chord y muy poco scrapers en causo de aumento pedidos en la red causa lentitud. Una buena proporcion seria de un scraper con 5 trabajadores disponibles por cada 2 o 3 nodos chord en la red. Una distribucion ideal seria un scraper por nodo chord.

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

