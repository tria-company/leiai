import { useState, useEffect, useRef } from 'react'
import { supabase } from './supabaseClient'
import { FileText, Download, Maximize2, X } from 'lucide-react'

function flattenForRow(dados) {
    const out = {}
    function walk(obj, prefix = '') {
        if (obj === null || obj === undefined) return
        if (Array.isArray(obj)) {
            out[prefix] = obj.length ? obj.join(', ') : ''
            return
        }
        if (typeof obj === 'object') {
            Object.entries(obj).forEach(([k, v]) => {
                const key = prefix ? `${prefix}_${k}` : k
                if (v !== null && typeof v === 'object' && !Array.isArray(v)) walk(v, key)
                else out[key] = Array.isArray(v) ? (v.length ? v.join(', ') : '') : (v ?? '')
            })
            return
        }
        out[prefix] = String(obj)
    }
    walk(dados)
    return out
}

function formatCell(value) {
    if (value === null || value === undefined) return '—'
    if (typeof value === 'boolean') return value ? 'Sim' : 'Não'
    if (typeof value === 'object') return Array.isArray(value) ? value.join(', ') : JSON.stringify(value)
    return String(value)
}

function ResultsViewer({ projetoId, onExportExcel }) {
    const [results, setResults] = useState([])
    const [isLoading, setIsLoading] = useState(false)
    const [isExpanded, setIsExpanded] = useState(false)
    const [selectedItem, setSelectedItem] = useState(null)
    const resultsRef = useRef([])
    const isLoadingRef = useRef(false)

    useEffect(() => {
        if (!projetoId || !supabase) return
        fetchResults(true)
        const channel = supabase
            .channel('realtime-results')
            .on('postgres_changes', { event: 'INSERT', schema: 'public', table: 'resultados_analise' }, () => fetchResults(false))
            .subscribe()
        const interval = setInterval(() => fetchResults(false), 20000)
        return () => {
            supabase.removeChannel(channel)
            clearInterval(interval)
        }
    }, [projetoId])

    async function fetchResults(showLoading) {
        if (!supabase) return
        if (isLoadingRef.current) return
        if (showLoading) {
            isLoadingRef.current = true
            setIsLoading(true)
        }
        try {
            let query = supabase
                .from('resultados_analise')
                .select('*')
                .order('data_processamento', { ascending: false })
                .limit(10000)
            if (projetoId) query = query.eq('projeto_id', projetoId)
            const { data, error } = await query
            if (error) throw error
            const list = data || []
            if (JSON.stringify(resultsRef.current) !== JSON.stringify(list)) {
                resultsRef.current = list
                setResults(list)
            }
        } catch (err) {
            console.error('Erro ao buscar resultados:', err)
        } finally {
            if (showLoading) {
                isLoadingRef.current = false
                setIsLoading(false)
            }
        }
    }

    const rows = results.map((r) => ({
        id: r.id,
        arquivo: r.arquivo_original || '—',
        data: r.data_processamento ? new Date(r.data_processamento).toLocaleString('pt-BR') : '—',
        ...flattenForRow(r.dados_json || {}),
        raw_json: r.dados_json
    }))

    const allKeys = new Set()
    rows.forEach((row) => Object.keys(row).forEach((k) => {
        if (k !== 'id' && k !== 'raw_json') allKeys.add(k)
    }))
    const columns = ['arquivo', 'data', ...Array.from(allKeys).filter((k) => k !== 'arquivo' && k !== 'data').sort()]

    const tableContent = (
        <>
            {isLoading ? (
                <div className="flex items-center justify-center py-12">
                    <div className="animate-spin rounded-full h-8 w-8 border-2 border-emerald-600 border-t-transparent" />
                    <span className="ml-3 text-sm text-slate-600">Carregando...</span>
                </div>
            ) : rows.length === 0 ? (
                <div className="text-center py-12 text-slate-500">
                    <FileText className="h-12 w-12 mx-auto mb-3 text-slate-300" />
                    <p className="text-sm">Nenhum resultado ainda</p>
                    <p className="text-xs mt-1">Os resultados aparecem aqui após o processamento</p>
                </div>
            ) : (
                <table className="min-w-full divide-y divide-slate-200 border-collapse">
                    <thead className="bg-slate-100 sticky top-0 z-10">
                        <tr>
                            {columns.map((col) => (
                                <th
                                    key={col}
                                    className="px-4 py-3 text-left text-xs font-semibold text-slate-700 uppercase tracking-wider whitespace-nowrap border-b border-slate-200"
                                >
                                    {col === 'arquivo' ? 'Arquivo' : col === 'data' ? 'Data' : col.replace(/_/g, ' ')}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-slate-200">
                        {rows.map((row, idx) => (
                            <tr
                                key={row.id || idx}
                                className="hover:bg-slate-50 cursor-pointer transition-colors"
                                onClick={() => setSelectedItem(row)}
                            >
                                {columns.map((col) => (
                                    <td key={col} className="px-4 py-2 text-sm text-slate-800 whitespace-nowrap border-b border-slate-100" title={formatCell(row[col])}>
                                        <span className="block max-w-xs truncate">{formatCell(row[col])}</span>
                                    </td>
                                ))}
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </>
    )

    return (
        <>
            <div className="bg-white rounded-xl shadow-lg border border-slate-200 overflow-hidden mt-10">
                <div className="px-6 py-4 bg-gradient-to-r from-emerald-50 to-teal-50 border-b border-slate-200 flex flex-wrap items-center justify-between gap-3">
                    <div className="flex items-center gap-3">
                        <FileText className="h-5 w-5 text-emerald-600 flex-shrink-0" />
                        <div>
                            <h3 className="font-bold text-slate-900 text-lg">Resultados das Análises</h3>
                            <p className="text-xs text-slate-600">
                                {results.length} resultado(s) • Clique na linha para detalhes
                            </p>
                        </div>
                    </div>
                    <div className="flex items-center gap-2">
                        <button
                            type="button"
                            onClick={() => setIsExpanded(true)}
                            disabled={rows.length === 0}
                            className="flex items-center gap-2 px-4 py-2 bg-slate-600 text-white rounded-lg hover:bg-slate-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors text-sm font-medium"
                        >
                            <Maximize2 className="h-4 w-4" />
                            Expandir Tabela
                        </button>
                        <button
                            onClick={onExportExcel}
                            disabled={results.length === 0}
                            className="flex items-center gap-2 px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:bg-slate-300 disabled:cursor-not-allowed transition-colors text-sm font-medium"
                        >
                            <Download className="h-4 w-4" />
                            Exportar XLSX
                        </button>
                    </div>
                </div>

                <div className="overflow-x-auto max-h-80">
                    {tableContent}
                </div>
            </div>

            {isExpanded && (
                <div
                    className="fixed inset-0 z-50 bg-slate-900/80 flex flex-col p-4"
                    onClick={() => setIsExpanded(false)}
                >
                    <div
                        className="bg-white rounded-xl shadow-xl flex flex-col flex-1 min-h-0 overflow-hidden"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="px-4 py-3 border-b border-slate-200 flex items-center justify-between shrink-0">
                            <h3 className="font-bold text-slate-900">Resultados — Visão Expandida</h3>
                            <button
                                type="button"
                                onClick={() => setIsExpanded(false)}
                                className="p-2 rounded-lg hover:bg-slate-100 text-slate-600"
                                aria-label="Fechar"
                            >
                                <X className="h-5 w-5" />
                            </button>
                        </div>
                        <div className="flex-1 overflow-auto p-4">
                            {tableContent}
                        </div>
                    </div>
                </div>
            )}

            {selectedItem && (
                <div
                    className="fixed inset-0 z-50 bg-slate-900/80 flex items-center justify-center p-4"
                    onClick={() => setSelectedItem(null)}
                >
                    <div
                        className="bg-white rounded-xl shadow-2xl w-full max-w-2xl max-h-[85vh] flex flex-col overflow-hidden animate-in fade-in zoom-in duration-200"
                        onClick={(e) => e.stopPropagation()}
                    >
                        <div className="px-6 py-4 border-b border-slate-200 flex items-center justify-between bg-slate-50">
                            <div>
                                <h3 className="font-bold text-slate-900 text-lg">Detalhes da Análise</h3>
                                <p className="text-xs text-slate-500 break-all">{selectedItem.arquivo}</p>
                            </div>
                            <button
                                type="button"
                                onClick={() => setSelectedItem(null)}
                                className="p-2 rounded-lg hover:bg-slate-200 text-slate-500 transition-colors"
                            >
                                <X className="h-5 w-5" />
                            </button>
                        </div>

                        <div className="flex-1 overflow-auto p-6 bg-slate-50/50">
                            <div className="bg-slate-900 rounded-lg p-4 overflow-x-auto border border-slate-700 shadow-inner">
                                <pre className="text-xs text-emerald-400 font-mono leading-relaxed">
                                    {JSON.stringify(selectedItem.raw_json, null, 2)}
                                </pre>
                            </div>
                        </div>

                        <div className="p-4 border-t border-slate-200 bg-slate-50 text-right">
                            <button
                                onClick={() => setSelectedItem(null)}
                                className="px-4 py-2 bg-slate-200 hover:bg-slate-300 text-slate-700 rounded-lg text-sm font-medium transition-colors"
                            >
                                Fechar
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </>
    )
}

export default ResultsViewer
