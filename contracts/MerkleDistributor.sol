// SPDX-License-Identifier: GPL-3.0
pragma solidity ^0.8.28;

import "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import "@openzeppelin/contracts/utils/cryptography/MerkleProof.sol";

contract MerkleDistributor {
    address public immutable token;
    bytes32 public immutable merkleRoot;
    mapping(uint256 => uint256) private claimedBitMap;
    event Claimed(uint256 index, address account, uint256 amount);

    constructor(address token_, bytes32 merkleRoot_) {
        token = token_;
        merkleRoot = merkleRoot_;
    }

    function isClaimed(uint256 index) public view returns (bool) {
        uint256 wordIndex = index >> 8;
        uint256 bitIndex = index & 0xff;
        uint256 word = claimedBitMap[wordIndex];
        return (word >> bitIndex) & 1 == 1;
    }

    function _setClaimed(uint256 index) private {
        uint256 wordIndex = index >> 8;
        uint256 bitIndex = index & 0xff;
        claimedBitMap[wordIndex] |= (1 << bitIndex);
    }

    function claim(
        uint256 index,
        address account,
        uint256 amount,
        bytes32[] calldata merkleProof
    ) external {
        require(!isClaimed(index), "MerkleDistributor: Drop already claimed");
        bytes32 node = keccak256(abi.encodePacked(index, account, amount));
        require(MerkleProof.verify(merkleProof, merkleRoot, node), "MerkleDistributor: Invalid proof");
        _setClaimed(index);
        require(IERC20(token).transfer(account, amount), "MerkleDistributor: Transfer failed");
        emit Claimed(index, account, amount);
    }
}
