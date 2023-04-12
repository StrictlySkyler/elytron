#!/bin/bash

npx babel src --presets babel-preset-es2015 --out-dir dist/src
npx babel lib --presets babel-preset-es2015 --out-dir dist/lib
