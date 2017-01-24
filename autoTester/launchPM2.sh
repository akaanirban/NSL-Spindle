#!/bin/bash
pm2 start --name autotester lib/index.js --interpreter ./node_modules/.bin/babel-node
