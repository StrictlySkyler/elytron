#!/bin/bash

mocha -w --compilers js:babel-core/register --recursive test/
