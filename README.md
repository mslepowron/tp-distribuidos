# tp-distribuidos
TP Grupal Sistemas Distribuidos 2C 2025

## Ejecucion
1. levantar servicios declarados en el compose
    ```bash
        ./run.sh
    ```

2. ver logs en vivo:
    ```bash
        docker compose -f docker-compose-dev.yaml logs -f <<service name>>
    ```
3. chequear que containers hay levantados:
   ```bash
        docker compose -f docker-compose-dev.yaml ps
    ```

4. Detener la ejecuci'on y bajar los containers:
    ```bash
        ./stop.sh
    ```

Se puede ver la UI de rabbit desde la web con: http://localhost:15672

## Tests Unitarios del Middleware
1. Levantar el docker compose de rabbit para los tests:
    ```bash
        docker compose -f docker-compose-tests.yaml up -d
    ```
2. Crear un entorno virtual para correr los tests: 
    ```bash
        python3 -m venv venv
        source venv/bin/activate
    ```
3. Instalar las dependencias necesarias
    ```bash
        pip install -r middleware/unit_tests/requirements.txt
    ```
4. Correr los tests desde el root del proyecto con
    ```bash
        pytest
    ```