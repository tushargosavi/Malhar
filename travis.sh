#!/bin/bash
mvn install -DskipTests=true
mvn test
cd ui
npm install .
npm test