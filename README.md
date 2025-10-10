# tp-distribuidos
TP Grupal Sistemas Distribuidos 2C 2025

## Ejecucion
1. Generar el archivo docker-compose con:
    ```bash
        make compose
    ```
La cantidad de cada tipo de nodos se especifica desde el archivo de configuracion global.

2. Ejecutar el sistema:
    ```bash
        make up
    ```

3. Ver logs en vivo
    ```bash
        make logs S=<<service name>>
    ```
4. Chequear que containers hay levantados:
   ```bash
        make ps
    ```

4. Detener la ejecucion y bajar los containers:
    ```bash
        make down
    ```

Se puede ver la UI de rabbit desde la web con: http://localhost:15672

## Tests Unitarios del Middleware
1. Levantar el docker compose de rabbit para los tests:
    ```bash
        make test-up
    ```
2. Crear un entorno virtual para correr los tests: 
    ```bash
        make venv
    ```
3. Correr los tests
    ```bash
        make test
    ```
5. Detener el container de RabbitMq de tests:
    ```bash
        make test-down
    ```
