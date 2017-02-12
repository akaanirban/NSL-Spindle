#!/bin/bash
pm2 start --name spindle-show-results lib/index.js --interpreter ./node_modules/.bin/babel-node
