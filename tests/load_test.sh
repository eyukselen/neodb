#!/bin/bash

locust -f load_testing.py --host http://localhost:8000 --users 100 --spawn-rate 100

