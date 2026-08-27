// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

/**
 * @title DatasetRegistry
 * @dev Public Ethereum L2 Commitment Registry for NVDA High-Throughput Trade Simulation Datasets.
 * Stores immutable cryptographic commitments (Dataset Hash, Merkle Root, URI, Trade Count).
 */
contract DatasetRegistry {
    struct DatasetCommitment {
        bytes32 datasetId;
        bytes32 datasetHash;
        bytes32 merkleRoot;
        uint64 tradeCount;
        uint64 timestamp;
        string uri;
        address committer;
    }

    // Mapping from datasetId => DatasetCommitment
    mapping(bytes32 => DatasetCommitment) public datasets;

    // Array of registered dataset IDs
    bytes32[] public datasetIds;

    event DatasetCommitted(
        bytes32 indexed datasetId,
        bytes32 indexed datasetHash,
        bytes32 indexed merkleRoot,
        uint64 tradeCount,
        uint64 timestamp,
        string uri,
        address committer
    );

    /**
     * @notice Registers a cryptographic commitment for a completed trade simulation dataset.
     */
    function commitDataset(
        bytes32 datasetId,
        bytes32 datasetHash,
        bytes32 merkleRoot,
        uint64 tradeCount,
        string calldata uri
    ) external {
        require(datasets[datasetId].timestamp == 0, "Dataset already registered.");
        require(tradeCount > 0, "Trade count must be positive.");

        DatasetCommitment memory commitment = DatasetCommitment({
            datasetId: datasetId,
            datasetHash: datasetHash,
            merkleRoot: merkleRoot,
            tradeCount: tradeCount,
            timestamp: uint64(block.timestamp),
            uri: uri,
            committer: msg.sender
        });

        datasets[datasetId] = commitment;
        datasetIds.push(datasetId);

        emit DatasetCommitted(
            datasetId,
            datasetHash,
            merkleRoot,
            tradeCount,
            uint64(block.timestamp),
            uri,
            msg.sender
        );
    }

    /**
     * @notice Returns total number of registered datasets.
     */
    function getDatasetCount() external view returns (uint256) {
        return datasetIds.length;
    }

    /**
     * @notice Retrieves commitment details for a given datasetId.
     */
    function getDataset(bytes32 datasetId) external view returns (
        bytes32 datasetHash,
        bytes32 merkleRoot,
        uint64 tradeCount,
        uint64 timestamp,
        string memory uri,
        address committer
    ) {
        DatasetCommitment memory d = datasets[datasetId];
        require(d.timestamp > 0, "Dataset not found.");
        return (d.datasetHash, d.merkleRoot, d.tradeCount, d.timestamp, d.uri, d.committer);
    }
}
