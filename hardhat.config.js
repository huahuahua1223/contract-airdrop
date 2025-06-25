require("dotenv").config();
require("@nomicfoundation/hardhat-toolbox");

const ALCHEMY_KEY = process.env.ALCHEMY_KEY;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
// npx hardhat run scripts/deploy.js --network arbitrum
// npx hardhat run scripts/deploy.js --network arbitrumTestnet

module.exports = {
  solidity: "0.8.28",
  networks: {
    arbitrum: {
      url: `https://arb-mainnet.g.alchemy.com/v2/${ALCHEMY_KEY}`,
      accounts: [PRIVATE_KEY],
    },
    arbitrumTestnet: {
      url: `https://arb-sepolia.g.alchemy.com/v2/${ALCHEMY_KEY}`,
      accounts: [PRIVATE_KEY],
    },
    sepolia: {
      url: `https://eth-sepolia.g.alchemy.com/v2/${ALCHEMY_KEY}`,
      accounts: [PRIVATE_KEY],
    },
  },
};
