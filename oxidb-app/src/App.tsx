import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom";
import { ThemeProvider } from "./context/ThemeContext";
import { ConnectionProvider, useConnection } from "./context/ConnectionContext";
import { ToastProvider } from "./components/common/Toast";
import { AppLayout } from "./components/layout/AppLayout";
import { ConnectionScreen } from "./components/connection/ConnectionScreen";
import { DashboardPage } from "./components/dashboard/DashboardPage";
import { CollectionBrowser } from "./components/collections/CollectionBrowser";
import { QueryEditor } from "./components/query/QueryEditor";
import { IndexManager } from "./components/indexes/IndexManager";
import { BlobManager } from "./components/blobs/BlobManager";
import { AggregationBuilder } from "./components/aggregation/AggregationBuilder";
import { ServerMonitor } from "./components/monitor/ServerMonitor";

function RequireConnection({ children }: { children: React.ReactNode }) {
  const { status } = useConnection();
  if (!status.connected) return <Navigate to="/" replace />;
  return <>{children}</>;
}

function AppRoutes() {
  return (
    <Routes>
      <Route path="/" element={<ConnectionScreen />} />
      <Route
        element={
          <RequireConnection>
            <AppLayout />
          </RequireConnection>
        }
      >
        <Route path="/dashboard" element={<DashboardPage />} />
        <Route path="/collections" element={<CollectionBrowser />} />
        <Route path="/query" element={<QueryEditor />} />
        <Route path="/indexes" element={<IndexManager />} />
        <Route path="/blobs" element={<BlobManager />} />
        <Route path="/aggregation" element={<AggregationBuilder />} />
        <Route path="/monitor" element={<ServerMonitor />} />
      </Route>
    </Routes>
  );
}

export default function App() {
  return (
    <ThemeProvider>
      <ConnectionProvider>
        <ToastProvider>
          <BrowserRouter>
            <AppRoutes />
          </BrowserRouter>
        </ToastProvider>
      </ConnectionProvider>
    </ThemeProvider>
  );
}
