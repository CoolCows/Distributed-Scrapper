* El cliente se comunica con chord, y chord se comunica con el scraper

* El cliente tiene un router para enviar pedido a sus nodos chord allegados. Tiene un delaer para recibir los pedidos desde cualquier nodo chord. Consiste solamente de 2 hilos, uno que se encarga de recibir las peticiones de los usuarios y otro para manejar la comunicacion entre nodo chord y cliente.

* El chord_scraper tiene un hilo para comunicarse con el cliente, otro para comunicarse con el scraper, hacerle pedidos de todavias estas online o requiero de tu servicios; ademas tiene un tercer hilo para recibir los trabajos que le han hecho sus scraper.
    - El hilo de comunicacion del cliente consiste en un solo nodo router para recibir las peticiones del cliente y enviarselas una vez haya terminado.
    - El hilo de comunicacion del scraper funciona idem al del cliente
    - EL hilo que recibe los trabajso tiene un push y un pull para enviar y recibir peticiones. El push y el pull estan bindeados por ahora dos posiciones mas arrbia que el puertp del nodo. push.port = node.port + 1 y pull.port = push.port + 1

* El scraper tiene un hilo para manejar las comunucaciones con el nodo chord. Ademas tiene un hilo por cada nodo chord que este utilizando sus servicios o los haya utilizado en un tiempo determinado.

## Falta
* Que los nodos tengan un tiempo de visibilidad (que esten disponible para cierta cantidad de conexiones?)
* Depurar comunicaciones intensamente:
    - Apagar un nodo cuando deje de funcionar
* Que hacer cuando da host unreacheable en el cliente
* Agregar el scrapeo en profundidad, chequear que el scraping funcione en talla
