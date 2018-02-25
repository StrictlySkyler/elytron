#!/bin/bash

babel src --presets babel-preset-es2015 --out-dir dist/src
babel lib --presets babel-preset-es2015 --out-dir dist/lib
