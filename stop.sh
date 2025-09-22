#!/bin/bash
docker-compose -f docker-compose-dev.yaml stop -t 1
docker-compose -f docker-compose-dev.yaml down