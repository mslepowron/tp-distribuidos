from gateway import Gateway

if __name__ == "__main__":
    gateway = Gateway("0.0.0.0", 9000)
    gateway.start()
