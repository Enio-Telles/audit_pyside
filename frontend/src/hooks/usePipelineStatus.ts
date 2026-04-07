import { useEffect } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { pipelineApi } from "../api/client";
import { useAppStore } from "../store/appStore";

export function usePipelineStatus() {
  const queryClient = useQueryClient();
  const { pipelinePolling, pipelineWatchCnpj, updatePipelineStatus } =
    useAppStore();

  useEffect(() => {
    if (!pipelinePolling || !pipelineWatchCnpj) return;
    const id = setInterval(async () => {
      const s = await pipelineApi.status(pipelineWatchCnpj);
      updatePipelineStatus(s);
      if (s.status === "done" || s.status === "error") {
        queryClient.invalidateQueries({
          queryKey: ["files", pipelineWatchCnpj],
        });
      }
    }, 1500);
    return () => clearInterval(id);
  }, [pipelinePolling, pipelineWatchCnpj, queryClient, updatePipelineStatus]);
}
