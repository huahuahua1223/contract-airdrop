// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

import "@openzeppelin/contracts/token/ERC20/ERC20.sol";

contract AirdropToken is ERC20 {
    constructor(uint256 initialSupply) ERC20("AirdropToken", "ADT") {
        // 发行初始总量给部署者
        _mint(msg.sender, initialSupply);
    }
}
