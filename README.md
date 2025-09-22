# tp-distribuidos
TP Grupal Sistemas Distribuidos 2C 2025

1. levantar servicios declarados en el compose (por ahora solo rabbit)
    ```bash
        ./run.sh
    ```

2. ver logs en vivo:
    ```bash
        docker compose -f docker-compose-dev.yaml logs -f rabbitmq
    ```
3. chequear si esta corriendo:
   ```bash
        docker compose -f docker-compose-dev.yaml ps
    ```

4. bajar rabbit:
    ```bash
        ./stop.sh
    ```

Se puede ver la UI de rabbit desde la web con: http://localhost:15672
