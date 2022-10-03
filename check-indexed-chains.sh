#!/bin/bash

# bitcoin
BTC_VERSION=$(curl -s https://api.github.com/repos/bitcoin/bitcoin/releases/latest | grep "tag_name" | awk '{print substr($2, 3, length($2)-4)}')
echo bitcoin-core=${BTC_VERSION}
sed -i "s/BTCD_VERSION=.*/BTCD_VERSION=${BTC_VERSION}/" docker/Dockerfile.bitcoin-core
# ethereum
ETH_VERSION=$(curl -s https://api.github.com/repos/ethereum/go-ethereum/releases/latest | grep "tag_name" | awk '{print substr($2, 3, length($2)-4)}')
ETH_SHA=$(curl -s https://api.github.com/repos/ethereum/go-ethereum/git/ref/tags/v${ETH_VERSION} | grep "sha" | awk '{print substr($2, 2, 8)}')
echo geth=${ETH_VERSION}-${ETH_SHA}
sed -i "s/GETH_VERSION=.*/GETH_VERSION=${ETH_VERSION}-${ETH_SHA}/" docker/Dockerfile.geth
