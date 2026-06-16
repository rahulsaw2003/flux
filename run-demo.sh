#!/bin/bash

# MiniKafka Demo Runner Script

echo "╔════════════════════════════════════════════════════════════════╗"
echo "║           MiniKafka Setup & Demo Runner                       ║"
echo "╚════════════════════════════════════════════════════════════════╝"
echo ""

# Check Java version
echo "Step 1: Checking Java version..."
JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d'.' -f1)

if [ "$JAVA_VERSION" != "23" ]; then
    echo "❌ Java 23 required. Current: Java $JAVA_VERSION"
    echo ""
    echo "Please run:"
    echo "  sdk use java 23-tem"
    echo ""
    exit 1
fi

echo "✓ Java 23 detected"
echo ""

# Clean previous data
echo "Step 2: Cleaning previous demo data..."
rm -rf data/
echo "✓ Cleaned"
echo ""

# Build project
echo "Step 3: Building MiniKafka..."
mvn clean compile -q

if [ $? -ne 0 ]; then
    echo "❌ Build failed. Please check errors above."
    exit 1
fi

echo "✓ Build successful"
echo ""

# Run demo
echo "Step 4: Starting Order Processing Demo..."
echo "════════════════════════════════════════════════════════════════"
echo ""

mvn exec:java -Dexec.mainClass="Main" -q

# Keep script running
wait
