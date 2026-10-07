import { lazy, StrictMode, Suspense } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router";
import { Layout } from "./components/Layout";
import { HomePage } from "./pages/HomePage";
import { PersonPage } from "./pages/PersonPage";
import { SearchPage } from "./pages/SearchPage";
import { SchoolPage } from "./pages/SchoolPage";
import { NotFoundPage } from "./pages/NotFoundPage";
import "./styles.css";

// the map brings d3-geo and the world's outlines, so it loads only when someone opens it
const MapPage = lazy(() => import("./pages/MapPage"));

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 5 * 60_000,
      retry: (count, err) => count < 2 && !(err instanceof Error && "status" in err && err.status === 404),
    },
  },
});

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<Layout />}>
            <Route index element={<HomePage />} />
            <Route path="search" element={<SearchPage />} />
            <Route path="m/:id" element={<PersonPage />} />
            <Route path="s/:id" element={<SchoolPage />} />
            <Route
              path="map"
              element={
                <Suspense fallback={<main className="page" />}>
                  <MapPage />
                </Suspense>
              }
            />
            <Route path="*" element={<NotFoundPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
