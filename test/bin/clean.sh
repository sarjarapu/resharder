#!/bin/bash

ps aux | grep -ie mongo | awk '{print $2}' | xargs kill -9
rm -rf ../data/*
