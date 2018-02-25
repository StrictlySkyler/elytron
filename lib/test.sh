#!/bin/bash

mocha --compilers js:babel-core/register --recursive test/
