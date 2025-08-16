import {
  DynamicContextProvider,
  DynamicWidget,
} from "@dynamic-labs/sdk-react-core";

import { EthereumWalletConnectors } from "@dynamic-labs/ethereum";
import { Route, Routes } from "react-router-dom";
import Home from "./pages/Home";


export default function App() {
  return (
    <DynamicContextProvider
      settings={{
        environmentId: "d1691841-4bf9-4724-97ab-547c16985465",
        walletConnectors: [EthereumWalletConnectors],
      }}
    >
      <DynamicWidget />
      <Routes>
        
        <Route path="/" element={<Home />} />
      </Routes>
    </DynamicContextProvider>
  );

}
