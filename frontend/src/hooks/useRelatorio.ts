import { useQuery } from '@tanstack/react-query';
import api from '../api/client';

export interface RelatorioData {
  id: string;
  title: string;
  content: string;
  createdAt: string;
}

export function useRelatorio(id: string) {
  return useQuery({
    queryKey: ['relatorio', id],
    queryFn: async (): Promise<RelatorioData> => {
      const { data } = await api.get(`/relatorios/${id}`);
      return data;
    },
    enabled: !!id,
  });
}
