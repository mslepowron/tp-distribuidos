import sys
import os
import configparser
import yaml
from copy import deepcopy

REQUIRED_ARGS = 2
NETWORK_NAME = "coffee_analysis_net"
SUBNET = "172.25.125.0/24"

def get_int(cfg, key, default=0):
    try:
        return cfg.getint("DEFAULT", key, fallback=default)
    except ValueError:
        return default

def define_header():
    return "services:\n"

def rabbitmq_config():
    return f"""  rabbitmq:
    build:
      context: ./middleware/rabbitmq
      dockerfile: rabbitmq.dockerfile
    ports:
      - 15672:15672
    networks:
      - {NETWORK_NAME}
    healthcheck:
      test: [ "CMD", "rabbitmqctl", "status" ]
      interval: 5s
      timeout: 10s
      retries: 5
    cpus: "1.0"
    mem_limit: 512m
"""

def app_controller():
    return f"""  app_controller:
    build:
      context: .
      dockerfile: app_controller/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - CONFIG_PATH=/config/config.ini
      - STORAGE_DIR=/storage
    volumes:
      - ./app_controller/config/config.ini:/config/config.ini:ro
      - ./app_controller/storage:/storage
    networks:
      - {NETWORK_NAME}
"""

def filter_year_block(i: int):
    return f'''  filter_year_{i}:
    build:
      context: .
      dockerfile: workers/filter/Dockerfile
    environment:
      - FILTER_TYPE=Year
      - QUEUE_NAME=filter_year_q_{i}
      - INPUT_BINDINGS=[["input_file_ex","direct","transactions"],["input_file_ex","direct","transaction_items"]]
      - OUTPUT_EXCHANGE=filter_year_ex
      - OUTPUT_RKS=["transactions", "transaction_items"]
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {NETWORK_NAME}
'''

def filter_hour_block(i: int):
    return f'''  filter_hour_{i}:
    build:
      context: .
      dockerfile: workers/filter/Dockerfile
    environment:
      - FILTER_TYPE=Hour
      - QUEUE_NAME=filter_hour_q_{i}
      - INPUT_BINDINGS=[["filter_year_ex","direct","transactions"],["join_stores_ex","direct","stores_joined"]]
      - OUTPUT_EXCHANGE=filter_hour_ex
      - OUTPUT_RKS=["transactions", "store_transactions"]
    depends_on:
      rabbitmq:
        condition: service_healthy
    networks:
      - {NETWORK_NAME}
'''

def filter_amount_block(i: int):
    return f'''  filter_amount_{i}:
    build:
      context: .
      dockerfile: workers/filter/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - FILTER_TYPE=Amount
      - QUEUE_NAME=filter_amount_q_{i}
      - INPUT_BINDINGS=[["filter_hour_ex","direct","transactions"]]
      - OUTPUT_EXCHANGE=results_ex
      - OUTPUT_RKS=["q_amount_75_tx"]
    networks:
      - {NETWORK_NAME}
'''

def join_stores_block(i: int):
    return f'''  join_stores_{i}:
    build:
      context: .
      dockerfile: workers/join/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - JOIN_TYPE=Store
      - QUEUE_NAME=join_store_q_{i}
      - INPUT_BINDINGS=[["input_file_ex","direct","stores"],["filter_year_ex","direct","transactions"]]
      - OUTPUT_EXCHANGE=join_stores_ex
      - OUTPUT_RKS=["stores_joined", "reduce_user_purchases"]
      - STORAGE_DIR=/storage
    volumes:
      - ./storage:/storage
    networks:
      - {NETWORK_NAME}
'''

def join_menu_block(i: int):
    return f'''  join_menu_{i}:
    build:
      context: .
      dockerfile: workers/join/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - JOIN_TYPE=Menu
      - QUEUE_NAME=join_menu_q_{i}
      - INPUT_BINDINGS=[["input_file_ex","direct","menu_items"],["filter_year_ex","direct","transaction_items"]]
      - OUTPUT_EXCHANGE=menu_items_ex
      - OUTPUT_RKS=["menu_items"]
      - STORAGE_DIR=/storage
    volumes:
      - ./storage:/storage
    networks:
      - {NETWORK_NAME}
'''

def join_users_block(i: int):
    return f'''  join_users_{i}:
    build:
      context: .
      dockerfile: workers/join/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - JOIN_TYPE=User
      - QUEUE_NAME=join_users_q_{i}
      - INPUT_BINDINGS=[["input_file_ex","direct","users"],["top_user_birthdays_ex","direct","top_user_birthdays"]]
      - OUTPUT_EXCHANGE=results_ex
      - OUTPUT_RKS=["q_top3_birthdays"]
      - STORAGE_DIR=/storage
    volumes:
      - ./storage:/storage
    networks:
      - {NETWORK_NAME}
'''

def map_year_month_block(i: int):
    return f'''  map_year_month_{i}:
    build:
      context: .
      dockerfile: workers/map/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MAP_TYPE=YearMonth
      - QUEUE_NAME=map_year_month_q_{i}
      - INPUT_BINDINGS=[["menu_items_ex","direct","menu_items"]]
      - OUTPUT_EXCHANGE=map_year_month_ex
      - OUTPUT_RKS=["quantity", "profit"]
    networks:
      - {NETWORK_NAME}
'''

def map_year_half_block(i: int):
    return f'''  map_year_half_{i}:
    build:
      context: .
      dockerfile: workers/map/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - MAP_TYPE=YearHalf
      - QUEUE_NAME=map_year_half_q_{i}
      - INPUT_BINDINGS=[["filter_hour_ex","direct","store_transactions"]]
      - OUTPUT_EXCHANGE=map_year_half_ex
      - OUTPUT_RKS=["map_half"]
    networks:
      - {NETWORK_NAME}
'''

def reduce_quantity_block(i: int):
    return f'''  reduce_quantity_{i}:
    build:
      context: .
      dockerfile: workers/reduce/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - REDUCE_TYPE=quantity
      - QUEUE_NAME=reduce_quantity_q_{i}
      - INPUT_BINDINGS=[["map_year_month_ex","direct","quantity"]]
      - OUTPUT_EXCHANGE=reduce_qty_ex
      - OUTPUT_RKS=["month_quantity"]
    networks:
      - {NETWORK_NAME}
'''

def reduce_profit_block(i: int):
    return f'''  reduce_profit_{i}:
    build:
      context: .
      dockerfile: workers/reduce/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - REDUCE_TYPE=profit
      - QUEUE_NAME=reduce_profit_q_{i}
      - INPUT_BINDINGS=[["map_year_month_ex","direct","profit"]]
      - OUTPUT_EXCHANGE=reduce_profit_ex
      - OUTPUT_RKS=["month_profit"]
    networks:
      - {NETWORK_NAME}
'''

def reduce_tpv_block(i: int):
    return f'''  reduce_tpv_{i}:
    build:
      context: .
      dockerfile: workers/reduce/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - REDUCE_TYPE=tpv
      - QUEUE_NAME=reduce_tpv_q_{i}
      - INPUT_BINDINGS=[["map_year_half_ex","direct","map_half"]]
      - OUTPUT_EXCHANGE=results_ex
      - OUTPUT_RKS=["q_semester_tpv"]
    networks:
      - {NETWORK_NAME}
'''

def reduce_user_purchases_block(i: int):
    return f'''  reduce_user_purchases_{i}:
    build:
      context: .
      dockerfile: workers/reduce/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - REDUCE_TYPE=user_purchases
      - QUEUE_NAME=reduce_user_purchases_q_{i}
      - INPUT_BINDINGS=[["join_stores_ex","direct","reduce_user_purchases"]]
      - OUTPUT_EXCHANGE=reduce_user_purchase_ex
      - OUTPUT_RKS=["top_user_purchase"]
    networks:
      - {NETWORK_NAME}
'''

def top_selling_block(i: int):
    return f'''  top_selling_items_{i}:
    build:
      context: .
      dockerfile: workers/top/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - TOP_TYPE=TopSellingItems
      - QUEUE_NAME=top_selling_items_q_{i}
      - INPUT_BINDINGS=[["reduce_qty_ex","direct","month_quantity"]]
      - OUTPUT_EXCHANGE=results_ex
      - OUTPUT_RKS=["q_month_top_qty"]
    networks:
      - {NETWORK_NAME}
'''

def top_revenue_block(i: int):
    return f'''  top_revenue_generating_items_{i}:
    build:
      context: .
      dockerfile: workers/top/Dockerfile
    depends_on:
      rabbitmq:
        condition: service_healthy
    environment:
      - TOP_TYPE=TopRevenueGeneratinItems
      - QUEUE_NAME=top_generating_items_q_{i}
      - INPUT_BINDINGS=[["reduce_profit_ex","direct","month_profit"]]
      - OUTPUT_EXCHANGE=results_ex
      - OUTPUT_RKS=["q_month_top_rev"]
    networks:
      - {NETWORK_NAME}
'''

def top_users_block(i: int):
    return f'''  top_store_user_purchases_{i}:
    build:
      context: .
      dockerfile: workers/top/Dockerfile
    depends_on:
      rabbitmq: 
        condition: service_healthy
    environment:
      - TOP_TYPE=TopStoreUserPurchases
      - QUEUE_NAME=top_store_user_purchases_q_{i}
      - INPUT_BINDINGS=[["reduce_user_purchase_ex","direct","top_user_purchase"]]
      - OUTPUT_EXCHANGE=top_user_birthdays_ex
      - OUTPUT_RKS=["top_user_birthdays"]
    networks:
      - {NETWORK_NAME}
'''

def gateway():
    return f'''  gateway:
    build:
      context: .
      dockerfile: gateway/Dockerfile
    depends_on:
      - app_controller
    environment:
      - APP_CONTROLLER_HOST=app_controller
      - APP_CONTROLLER_PORT=9100
    ports:
      - "9000:9000"
      - "9200:9200"
    networks:
      - {NETWORK_NAME}
'''

def networks():
    return f'''networks:
  {NETWORK_NAME}:
    ipam:
      driver: default
      config:
        - subnet: {SUBNET}
'''

def client_block(idx: int):
    # Todos leen la MISMA carpeta de datos
    data_host = "./client/data"
    report_host = f"./client/report/client_{idx}"
    os.makedirs(report_host, exist_ok=True)
    return f'''  client_{idx}:
    build:
      context: .
      dockerfile: client/Dockerfile
    depends_on:
      - gateway
    environment:
      - GATEWAY_HOST=gateway
      - GATEWAY_PORT=9000
      - BATCH_SIZE=50000
      - RAW_DATA_DIR=/data
      - OUTPUT_DIR=/report
    volumes:
      - {data_host}:/data:ro
      - {report_host}:/report
    networks:
      - {NETWORK_NAME}
'''

def generate_docker_compose(file):
    config = configparser.ConfigParser()
    read = config.read("global_config.ini")
    if not read:
        print("Error: global_config.ini not found.")
        sys.exit(1)

    FILTER_YEAR_NODES = get_int(config, "FILTER_YEAR_NODES", 1)
    FILTER_HOUR_NODES = get_int(config, "FILTER_HOUR_NODES", 1)
    FILTER_AMOUNT_NODES = get_int(config, "FILTER_AMOUNT_NODES", 1)

    JOIN_STORES_NODES = get_int(config, "JOIN_STORES_NODES", 1)
    JOIN_MENU_NODES = get_int(config, "JOIN_MENU_NODES", 1)
    JOIN_USERS_NODES = get_int(config, "JOIN_USERS_NODES", 1)

    MAP_YEAR_MONTH_NODES = get_int(config, "MAP_YEAR_MONTH_NODES", 1)
    MAP_YEAR_SEMESTER_NODES = get_int(config, "MAP_YEAR_SEMESTER_NODES", 1)

    REDUCE_QUANTITY_NODES = get_int(config, "REDUCE_QUANTITY_NODES", 1)
    REDUCE_PROFIT_NODES = get_int(config, "REDUCE_PROFIT_NODES", 1)
    REDUCE_TPV_NODES = get_int(config, "REDUCE_TPV_NODES", 1)
    REDUCE_USER_PURCHASES_NODES = get_int(config, "REDUCE_USER_PURCHASES_NODES", 1)

    TOP_SELLING_NODES = get_int(config, "TOP_SELLING_NODES", 1)
    TOP_REVENUE_NODES = get_int(config, "TOP_REVENUE_NODES", 1)
    TOP_USERS_NODES = get_int(config, "TOP_USERS_NODES", 1)

    CLIENT_NODES = get_int(config, "CLIENT_NODES", 1)


    compose = define_header()
    compose += rabbitmq_config()
    compose += app_controller()
    compose += gateway()

    for i in range(1, FILTER_YEAR_NODES + 1):
        compose += filter_year_block(i)
    for i in range(1, FILTER_HOUR_NODES + 1):
        compose += filter_hour_block(i)
    for i in range(1, FILTER_AMOUNT_NODES + 1):
        compose += filter_amount_block(i)

    for i in range(1, JOIN_STORES_NODES + 1):
        compose += join_stores_block(i)
    for i in range(1, JOIN_MENU_NODES + 1):
        compose += join_menu_block(i)
    for i in range(1, JOIN_USERS_NODES + 1):
        compose += join_users_block(i)

    for i in range(1, MAP_YEAR_MONTH_NODES + 1):
        compose += map_year_month_block(i)
    for i in range(1, MAP_YEAR_SEMESTER_NODES + 1):
        compose += map_year_half_block(i)

    for i in range(1, REDUCE_QUANTITY_NODES + 1):
        compose += reduce_quantity_block(i)
    for i in range(1, REDUCE_PROFIT_NODES + 1):
        compose += reduce_profit_block(i)
    for i in range(1, REDUCE_TPV_NODES + 1):
        compose += reduce_tpv_block(i)
    for i in range(1, REDUCE_USER_PURCHASES_NODES + 1):
        compose += reduce_user_purchases_block(i)

    for i in range(1, TOP_SELLING_NODES + 1):
        compose += top_selling_block(i)
    for i in range(1, TOP_REVENUE_NODES + 1):
        compose += top_revenue_block(i)
    for i in range(1, TOP_USERS_NODES + 1):
        compose += top_users_block(i)

    for i in range(1, CLIENT_NODES + 1):
        compose += client_block(i)

    compose += networks()

    with open(file, 'w') as f:
        f.write(compose)

    print(f"Docker Compose file '{file}' generated with {CLIENT_NODES} clients.")

if __name__ == "__main__":
    if len(sys.argv) < REQUIRED_ARGS:
        print("Use: python3 generate-compose.py <output.yml>")
        sys.exit(2)
    file = sys.argv[1]
    generate_docker_compose(file)