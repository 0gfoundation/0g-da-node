# 0G Data Availability Node

## Overview

0G DA is a modular data availability layer designed for high-performance and high-throughput chains and rollups, particularly in AI and gaming. 


## Architecture

0G DA consists of the following modules:

1. Data Availability Nodes and Signers on the DA network
2. Data Availability Clients and Encoders on the rollup/appchain side for data dispersion

Across the two sets of nodes, 0G DA supports the following features:

- **Erasure Coding**: Data is split into chunks and distributed across Storage Nodes.
- **KZG Commitments**: Uses the AMT protocol to reduce overhead when verifying KZG commitments.
- **Verifiable Random Function (VRF)**: Ensures unpredictable and verifiable selection of DA Nodes for availability sampling.
- **Quorum-based and Sampling-basedVerification**: Small groups of DA nodes work together to check and efficiently verify stored data.
- **Separate Validator Network**: Validators finalize proofs submitted by DA nodes.

For in-depth technical details about 0G DA, please read our [Intro to 0G DA](https://docs.0g.ai/concepts/da).

## Documentation

- If you want to run a node, please refer to the [Running a Node](https://docs.0g.ai/run-a-node/da-node) guide.
- If you learn more about 0G DA, please refer to the [0G DA Technical Deep Dive](https://docs.0g.ai/developer-hub/building-on-0g/da-deep-dive) guide.
- If you want build a rollup with 0G, please refer to the [OP Stack Rollup](https://docs.0g.ai/build-with-0g/rollups-and-appchains/op-stack-on-0g-da) or [Nitro Rollup](https://docs.0g.ai/developer-hub/building-on-0g/rollups-and-appchains/arbitrum-nitro-on-0g-da) guides.

## Support and Additional Resources
We want to do everything we can to help you be successful while working on your contribution and projects. Here you'll find various resources and communities that may help you complete a project or contribute to 0G. 

### Communities
- [0G Telegram](https://t.me/web3_0glabs)
- [0G Discord](https://discord.com/invite/0glabs)
