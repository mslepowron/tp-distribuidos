FROM rabbitmq-python-base:0.0.1

WORKDIR /root/tp-distribuidos

COPY . .

ENV PYTHONPATH=/root/tp-distribuidos

CMD ["python3", "workers/top/main.py"]
