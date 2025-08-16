import {
  DynamicContextProvider,
  useDynamicContext,
} from "@dynamic-labs/sdk-react-core";

import { EthereumWalletConnectors } from "@dynamic-labs/ethereum";
import { Navigate, Route, Routes } from "react-router-dom";
import Home from "./pages/Home";
import Auth from "./pages/Auth";
import About from "./pages/About";
import Contact from "./pages/Contact";
import Navbar from "./components/Navbar";

export default function App() {
  return (
    <DynamicContextProvider
      settings={{
        environmentId: "d1691841-4bf9-4724-97ab-547c16985465",
        walletConnectors: [EthereumWalletConnectors],
      }}
    >
      <Navbar />
      <Routes>
        <Route path="/" element={<AuthRedirector />} />
        <Route path="/home" element={<Home />} />
        <Route path="/about" element={<About />} />
        <Route path="/contact" element={<Contact />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </DynamicContextProvider>
  );

}

function AuthRedirector() {
  const { user } = useDynamicContext();
  if (user) {
    return <Navigate to="/home" replace />;
  }
  return <Auth />;
}
