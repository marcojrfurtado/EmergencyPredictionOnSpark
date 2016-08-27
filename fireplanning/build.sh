#!/bin/bash
cd utils

#Build CUDA
RESOURCES_FOLDER=src/main/resources
nvcc -ptx $RESOURCES_FOLDER/MatchEmergencyCalls.cu -o $RESOURCES_FOLDER/MatchEmergencyCalls.ptx


# Build scala and package it
sbt package
