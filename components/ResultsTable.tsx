
import React from 'react';
import { MatchResult } from '../types';

interface ResultsTableProps {
  results: MatchResult[];
}

const ResultsTable: React.FC<ResultsTableProps> = ({ results }) => {
  return (
    <div className="overflow-x-auto rounded-2xl border border-slate-200 shadow-sm bg-white">
      <table className="w-full text-left text-sm border-collapse">
        <thead className="bg-slate-50 text-slate-500 uppercase font-semibold border-b border-slate-200">
          <tr>
            <th className="px-6 py-4 min-w-[200px]">Source Filename</th>
            <th className="px-6 py-4">Status</th>
            <th className="px-6 py-4">Extracted Clr</th>
            <th className="px-6 py-4">Validated Code</th>
            <th className="px-6 py-4 min-w-[280px]">Matching Analysis</th>
            <th className="px-6 py-4">ERP Match Details</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-100">
          {results.map((res, idx) => (
            <tr key={idx} className="hover:bg-slate-50/50 transition-colors">
              <td className="px-6 py-4">
                <div className="flex flex-col gap-1">
                  <span className="font-mono text-[11px] text-slate-600 break-all max-w-[200px]" title={res.fileName}>
                    {res.fileName}
                  </span>
                  <div className="flex items-center gap-2">
                    <span className="text-[10px] bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded font-bold">
                      {res.brandHint}
                    </span>
                  </div>
                </div>
              </td>
              <td className="px-6 py-4">
                <span className={`px-2 py-1 rounded-full text-[10px] font-bold uppercase tracking-wider ${
                  res.status === 'SUCCESS' ? 'bg-green-100 text-green-700' :
                  res.status === 'FUZZY' ? 'bg-orange-100 text-orange-700' :
                  'bg-red-100 text-red-700'
                }`}>
                  {res.status}
                </span>
              </td>
              <td className="px-6 py-4">
                <span className={`font-bold font-mono text-xs ${res.colorCode ? 'text-indigo-600 bg-indigo-50 px-2 py-1 rounded' : 'text-slate-300'}`}>
                  {res.colorCode || 'None'}
                </span>
              </td>
              <td className="px-6 py-4">
                <div className="flex flex-col">
                  <span className="font-bold text-slate-800 text-sm">{res.productCode}</span>
                  {res.isFuzzy && (
                    <span className="text-[10px] text-orange-600 font-medium">
                      Matched to: {res.fuzzyMatchCode}
                    </span>
                  )}
                </div>
              </td>
              <td className="px-6 py-4">
                <div className="space-y-1.5">
                  <p className={`text-[11px] leading-tight ${res.status === 'FAILURE' ? 'text-red-500 font-medium' : 'text-slate-600'}`}>
                    {res.reason}
                  </p>
                  <div className="flex flex-wrap gap-1">
                    {res.attempts.map((att, i) => (
                      <span key={i} className={`text-[9px] px-1 rounded font-mono border ${
                        res.productCode === att ? 'bg-indigo-600 border-indigo-600 text-white' : 'bg-slate-50 border-slate-200 text-slate-400'
                      }`}>
                        {att}
                      </span>
                    ))}
                  </div>
                </div>
              </td>
              <td className="px-6 py-4">
                {res.isMatch ? (
                  <div className="flex flex-col gap-1">
                    <span className="text-slate-900 font-bold text-xs">{res.erpProductName}</span>
                    <div className="flex items-center gap-2">
                      <span className="text-[10px] bg-slate-100 px-1 rounded text-slate-500 font-medium border border-slate-200">{res.erpStyleCategory}</span>
                      <span className="text-[10px] text-slate-400 font-medium">{res.erpBrand}</span>
                    </div>
                  </div>
                ) : (
                  <span className="text-slate-300 italic text-xs">No data</span>
                )}
              </td>
            </tr>
          ))}
          {results.length === 0 && (
            <tr>
              <td colSpan={6} className="px-6 py-20 text-center">
                <div className="flex flex-col items-center gap-2 text-slate-400">
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-10 w-10 opacity-20" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                  </svg>
                  <p className="italic font-medium">Upload imagery to automatically query ERP data.</p>
                </div>
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
};

export default ResultsTable;
