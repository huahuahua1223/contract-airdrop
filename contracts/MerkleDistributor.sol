// SPDX-License-Identifier: GPL-3.0
pragma solidity ^0.8.20;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/utils/cryptography/MerkleProof.sol";
import "@openzeppelin/contracts/access/Ownable.sol";

contract MerkleDistributor is Ownable {
    address public immutable token;
    
    // 顶层Merkle树根
    bytes32 public merkleRoot;
    
    // 批次根映射
    mapping(uint256 => bytes32) public batchRoots;
    
    // 已验证的批次数量
    uint256 public verifiedBatchCount;
    
    // 已领取的记录
    mapping(uint256 => uint256) private claimedBitMap;
    
    event Claimed(uint256 index, address account, uint256 amount);
    event BatchRootAdded(uint256 batchIndex, bytes32 batchRoot);

    constructor(address token_, bytes32 merkleRoot_) Ownable(msg.sender) {
        token = token_;
        merkleRoot = merkleRoot_;
    }

    /**
     * @dev 判断指定索引是否已领取
     */
    function isClaimed(uint256 index) public view returns (bool) {
        uint256 wordIndex = index >> 8;
        uint256 bitIndex = index & 0xff;
        uint256 word = claimedBitMap[wordIndex];
        return (word >> bitIndex) & 1 == 1;
    }

    /**
     * @dev 标记指定索引为已领取
     */
    function _setClaimed(uint256 index) private {
        uint256 wordIndex = index >> 8;
        uint256 bitIndex = index & 0xff;
        claimedBitMap[wordIndex] |= (1 << bitIndex);
    }

    /**
     * @dev 设置顶层Merkle树根
     */
    function setMerkleRoot(bytes32 _merkleRoot) external onlyOwner {
        merkleRoot = _merkleRoot;
    }
    
    /**
     * @dev 添加批次根
     */
    function addBatchRoot(uint256 batchIndex, bytes32 batchRoot) external onlyOwner {
        require(batchRoots[batchIndex] == bytes32(0), "MerkleDistributor: Batch root already exists");
        batchRoots[batchIndex] = batchRoot;
        verifiedBatchCount++;
        emit BatchRootAdded(batchIndex, batchRoot);
    }
    
    /**
     * @dev 批量添加批次根
     */
    function addBatchRoots(uint256[] calldata batchIndices, bytes32[] calldata roots) external onlyOwner {
        require(batchIndices.length == roots.length, "MerkleDistributor: Array lengths mismatch");
        
        for (uint256 i = 0; i < batchIndices.length; i++) {
            uint256 batchIndex = batchIndices[i];
            bytes32 batchRoot = roots[i];
            
            if (batchRoots[batchIndex] == bytes32(0)) {
                batchRoots[batchIndex] = batchRoot;
                verifiedBatchCount++;
                emit BatchRootAdded(batchIndex, batchRoot);
            }
        }
    }

    /**
     * @dev 标准索引领取方法 - 与原始实现兼容
     */
    function claim(
        uint256 index,
        address account,
        uint256 amount,
        bytes32[] calldata merkleProof
    ) external {
        require(!isClaimed(index), "MerkleDistributor: Drop already claimed");
        
        // 验证顶层Merkle树证明
        bytes32 node = keccak256(abi.encodePacked(index, account, amount));
        require(MerkleProof.verify(merkleProof, merkleRoot, node), "MerkleDistributor: Invalid proof");
        
        _setClaimed(index);
        require(IERC20(token).transfer(account, amount), "MerkleDistributor: Transfer failed");
        emit Claimed(index, account, amount);
    }
    
    /**
     * @dev 支持两层Merkle树的领取方法
     */
    function claimFromBatch(
        uint256 index,
        uint256 batchIndex,
        address account,
        uint256 amount,
        bytes32[] calldata merkleProof
    ) external {
        require(!isClaimed(index), "MerkleDistributor: Drop already claimed");
        require(batchRoots[batchIndex] != bytes32(0), "MerkleDistributor: Batch root not verified");
        
        // 验证批次内的Merkle证明
        bytes32 node = keccak256(abi.encodePacked(index, account, amount));
        require(MerkleProof.verify(merkleProof, batchRoots[batchIndex], node), "MerkleDistributor: Invalid proof");
        
        _setClaimed(index);
        require(IERC20(token).transfer(account, amount), "MerkleDistributor: Transfer failed");
        emit Claimed(index, account, amount);
    }
    
    /**
     * @dev 领取多笔空投（针对同一地址多次出现在列表中的情况）
     */
    function claimMultiple(
        uint256[] calldata indices,
        uint256[] calldata batchIndices,
        address account,
        uint256[] calldata amounts,
        bytes32[][] calldata merkleProofs
    ) external {
        require(
            indices.length == batchIndices.length && 
            indices.length == amounts.length && 
            indices.length == merkleProofs.length,
            "MerkleDistributor: Array lengths mismatch"
        );
        
        uint256 totalAmount = 0;
        
        for (uint256 i = 0; i < indices.length; i++) {
            uint256 index = indices[i];
            uint256 batchIndex = batchIndices[i];
            uint256 amount = amounts[i];
            bytes32[] calldata proof = merkleProofs[i];
            
            require(!isClaimed(index), "MerkleDistributor: Drop already claimed");
            require(batchRoots[batchIndex] != bytes32(0), "MerkleDistributor: Batch root not verified");
            
            // 验证批次内的Merkle证明
            bytes32 node = keccak256(abi.encodePacked(index, account, amount));
            require(MerkleProof.verify(proof, batchRoots[batchIndex], node), "MerkleDistributor: Invalid proof");
            
            _setClaimed(index);
            totalAmount += amount;
            
            emit Claimed(index, account, amount);
        }
        
        require(IERC20(token).transfer(account, totalAmount), "MerkleDistributor: Transfer failed");
    }
    
    /**
     * @dev 紧急提取合约中的代币（仅所有者可调用）
     */
    function rescueTokens(address to, uint256 amount) external onlyOwner {
        require(IERC20(token).transfer(to, amount), "MerkleDistributor: Transfer failed");
    }
}
