import { useQuery } from '@tanstack/react-query';
import { pipelineApi } from '../../api/client';
import { useAppStore } from '../../store/appStore';

export function LogsTab() {
  const { selectedCnpj } = useAppStore();

  const { data } = useQuery({
    queryKey: ['pipelineStatus', selectedCnpj],
    queryFn: () => pipelineApi.status(selectedCnpj!),
    enabled: !!selectedCnpj,
    refetchInterval: 3000,
  });

  return (
    <div className="flex flex-col h-full p-3">
      <div className="text-xs text-slate-400 mb-2 font-semibold uppercase tracking-wide">Logs do Pipeline</div>
      {selectedCnpj ? (
        <>
          <div className="flex items-center gap-2 mb-2">
            <span
              className={`text-xs px-2 py-0.5 rounded font-semibold ${
                data?.status === 'done'
                  ? 'bg-green-800 text-green-200'
                  : data?.status === 'error'
                  ? 'bg-red-800 text-red-200'
                  : data?.status === 'running'
                  ? 'bg-yellow-800 text-yellow-200'
                  : 'bg-slate-700 text-slate-300'
              }`}
            >
              {data?.status?.toUpperCase() ?? 'IDLE'}
            </span>
          </div>
          <div
            className="flex-1 overflow-auto rounded p-3 text-xs font-mono"
            style={{ background: '#060e1f', border: '1px solid #1e3a5f' }}
          >
            {(data?.progresso ?? []).map((msg, i) => (
              <div key={i} className="text-slate-300 leading-5">{msg}</div>
            ))}
            {(data?.erros ?? []).map((e, i) => (
              <div key={`e${i}`} className="text-red-400 leading-5">[ERRO] {e}</div>
            ))}
            {(!data?.progresso?.length && !data?.erros?.length) && (
              <div className="text-slate-600">Nenhum log disponível.</div>
            )}
          </div>
        </>
      ) : (
        <div className="text-slate-500 text-sm">Selecione um CNPJ para ver os logs.</div>
      )}
    </div>
  );
}
