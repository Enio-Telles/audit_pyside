import { renderHook } from "@testing-library/react";
import { vi, describe, it, expect, beforeEach, afterEach } from "vitest";
import { createElement } from "react";
import { usePipelineStatus } from "../usePipelineStatus";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";

function makeWrapper(queryClient: QueryClient) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return createElement(QueryClientProvider, { client: queryClient }, children);
  };
}

vi.mock("../../api/client", () => ({
  pipelineApi: {
    status: vi.fn(),
  },
}));

vi.mock("../../store/appStore", () => ({
  useAppStore: vi.fn(() => ({
    pipelinePolling: false,
    pipelineWatchCnpj: null,
    updatePipelineStatus: vi.fn(),
  })),
}));

describe("usePipelineStatus", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("não faz polling quando pipelinePolling é false", async () => {
    const { pipelineApi } = await import("../../api/client");
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    renderHook(() => usePipelineStatus(), { wrapper: makeWrapper(qc) });

    vi.advanceTimersByTime(5000);

    expect(pipelineApi.status).not.toHaveBeenCalled();
  });

  it("exporta a função usePipelineStatus como função", async () => {
    const mod = await import("../usePipelineStatus");
    expect(typeof mod.usePipelineStatus).toBe("function");
  });
});
