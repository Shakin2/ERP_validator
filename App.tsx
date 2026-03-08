import React, { useState, useCallback } from 'react';
import FileUploadZone from './components/FileUploadZone';
import ResultsTable from './components/ResultsTable';
import { ERPRecord, MatchResult, FileStatus, ExtractedInfo, QueryLog } from './types';
import { parseFilename, parseCSV } from './utils/parser';
import { findBestFuzzyMatch } from './utils/fuzzy';

// Configuration Defaults
const DEFAULT_DATABRICKS_HOST = "adb-4215830024773554.14.azuredatabricks.net";
const DEFAULT_HTTP_PATH = "b848132ea9c3d9df";
const MAX_RETRIES = 3;
const LARGE_BATCH_SIZE = 4000;

const App: React.FC = () => {
  const [results, setResults] = useState<MatchResult[]>([]);
  const [status, setStatus] = useState<FileStatus>(FileStatus.IDLE);
  const [syncError, setSyncError] = useState<string | null>(null);
  const [debugLogs, setDebugLogs] = useState<QueryLog[]>([]);

  // Configuration State
  const [host, setHost] = useState(DEFAULT_DATABRICKS_HOST);
  const [path, setPath] = useState(DEFAULT_HTTP_PATH);
  const [token, setToken] = useState(""); 

  const PROXY_URL = "https://databricks-proxy-725829572897.europe-west1.run.app"; 

  // Helper: Remove brackets from product codes
  const cleanProductCode = (code: string): string => {
    return code.replace(/[\(\)\[\]\{\}]/g, '').trim();
  };

  // Helper: Chunk array to ensure we don't break SQL parameter limits if list is massive
  const chunkArray = (array: string[], size: number) => {
    const chunked = [];
    for (let i = 0; i < array.length; i += size) {
      chunked.push(array.slice(i, i + size));
    }
    return chunked;
  };

  // Helper: Retry Wrapper
  const fetchWithRetry = async <T,>(fn: () => Promise<T>, retries: number = MAX_RETRIES, delay: number = 1000): Promise<T> => {
    try {
      return await fn();
    } catch (err) {
      if (retries > 0) {
        console.warn(`Request failed. Retrying in ${delay}ms... (${retries} attempts left)`);
        await new Promise(res => setTimeout(res, delay));
        return fetchWithRetry(fn, retries - 1, delay * 1.5);
      }
      throw err;
    }
  };

  // Helper: Count color components in a code (e.g., "BLK-SIL" has 2, "BLK" has 1)
  const countColorComponents = (code: string): number => {
    const parts = code.split('-');
    if (parts.length <= 1) return 0;
    return parts.slice(1).filter(p => p.length > 0).length;
  };

  // Helper: Extract color hint from filename based on position after style code
  const extractColorHintFromFilename = (fileName: string, styleCode: string): string | null => {
    const nameWithoutExt = fileName.replace(/\.[^.]+$/, '');
    const parts = nameWithoutExt.split(/[_\s]+/);
    
    // Find the part that matches the style code
    const styleIndex = parts.findIndex(p => 
      cleanProductCode(p.toUpperCase()) === styleCode ||
      p.toUpperCase() === styleCode
    );
    
    if (styleIndex >= 0 && styleIndex + 1 < parts.length) {
      const colorPart = parts[styleIndex + 1].toUpperCase().trim();
      // Skip known non-color parts
      const NON_COLOR_PARTS = new Set([
        'HERO', 'DETAIL', 'BACK', 'FRONT', 'SIDE', 'TOP', 'BOTTOM',
        'LG', 'SM', 'MD', 'XL', 'LARGE', 'SMALL', 'MEDIUM',
        'THUMBNAIL', 'MAIN', 'ALT', 'IMG', 'IMAGE', 'PHOTO', 'PIC'
      ]);
      if (colorPart && !NON_COLOR_PARTS.has(colorPart)) {
        return colorPart;
      }
    }
    return null;
  };

  const executeDatabricksQuery = async (query: string, batchId: number, stage: 'KEYS' | 'DETAILS'): Promise<{ data: any[], log: QueryLog }> => {
    if (!token) throw new Error("Missing Access Token");
    const warehouseId = path.split('/').pop();

    const logEntry: QueryLog = {
      batchId,
      stage,
      timestamp: new Date().toISOString(),
      statement: query,
      rawResponse: null,
      parsedCount: 0
    };

    try {
      console.log(`[Batch ${batchId}] Submitting query...`);
      
      const controller = new AbortController();
      const timeout30s = setTimeout(() => controller.abort(), 30000);
      
      let response: Response;
      try {
        response = await fetch(PROXY_URL, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            host: host,
            warehouseId: warehouseId,
            token: token,
            statement: query
          }),
          signal: controller.signal
        });
      } finally {
        clearTimeout(timeout30s);
      }

      if (!response.ok) {
        const errorData = await response.json();
        logEntry.error = errorData.message || "HTTP Error";
        logEntry.rawResponse = errorData;
        throw new Error(logEntry.error);
      }

      let result = await response.json();
      logEntry.rawResponse = result;
      
      console.log(`[Batch ${batchId}] Initial response state: ${result.status?.state}`);
      
      if (result.status?.state === 'PENDING' || result.status?.state === 'RUNNING') {
        const statementId = result.statement_id;
        
        if (!statementId) {
          console.error(`[Batch ${batchId}] No statement_id in response:`, result);
          throw new Error("No statement_id returned for pending query");
        }

        console.log(`[Batch ${batchId}] Query ${statementId} is ${result.status.state}. Starting polling...`);
        
        const maxPollingAttempts = 120;
        const pollingInterval = 5000;
        
        for (let attempt = 0; attempt < maxPollingAttempts; attempt++) {
          await new Promise(res => setTimeout(res, pollingInterval));
          
          console.log(`[Batch ${batchId}] Polling attempt ${attempt + 1}/${maxPollingAttempts}...`);
          
          const pollController = new AbortController();
          const pollTimeout = setTimeout(() => pollController.abort(), 15000);
          
          let pollResponse: Response;
          try {
            pollResponse = await fetch(PROXY_URL, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                host: host,
                warehouseId: warehouseId,
                token: token,
                statement_id: statementId,
                poll: true
              }),
              signal: pollController.signal
            });
          } catch (pollErr: any) {
            if (pollErr.name === 'AbortError') {
              console.warn(`[Batch ${batchId}] Poll attempt ${attempt + 1} timed out, retrying...`);
              continue;
            }
            console.error(`[Batch ${batchId}] Poll error:`, pollErr);
            throw pollErr;
          } finally {
            clearTimeout(pollTimeout);
          }

          if (!pollResponse.ok) {
            const errorData = await pollResponse.json();
            console.error(`[Batch ${batchId}] Poll response error:`, errorData);
            throw new Error(`Polling failed: ${errorData.message || 'Unknown error'}`);
          }

          result = await pollResponse.json();
          logEntry.rawResponse = result;
          
          const state = result.status?.state;
          console.log(`[Batch ${batchId}] Poll result - Status: ${state}`);
          
          if (state === 'SUCCEEDED') {
            console.log(`[Batch ${batchId}] ✅ Query completed successfully after ${((attempt + 1) * pollingInterval / 1000).toFixed(0)}s`);
            break;
          } else if (state === 'FAILED' || state === 'CANCELED') {
            const errorMsg = result.status?.error?.message || 'Unknown error';
            console.error(`[Batch ${batchId}] ❌ Query ${state}: ${errorMsg}`);
            throw new Error(`Query ${state}: ${errorMsg}`);
          } else if (state === 'PENDING' || state === 'RUNNING') {
            continue;
          } else {
            console.error(`[Batch ${batchId}] Unknown state: ${state}`, result);
            throw new Error(`Unknown query state: ${state}`);
          }
        }
        
        if (result.status?.state !== 'SUCCEEDED') {
          console.error(`[Batch ${batchId}] Polling timeout. Final state: ${result.status?.state}`);
          throw new Error(`Query did not complete within ${maxPollingAttempts * pollingInterval / 1000}s. Last state: ${result.status?.state}`);
        }
      } else if (result.status?.state === 'SUCCEEDED') {
        console.log(`[Batch ${batchId}] ✅ Query completed immediately`);
      } else {
        console.warn(`[Batch ${batchId}] Unexpected initial state: ${result.status?.state}`);
      }
      
      if (!result.manifest || !result.result?.data_array) {
        console.warn(`[Batch ${batchId}] No data in response. Manifest: ${!!result.manifest}, data_array: ${!!result.result?.data_array}`);
        console.log(`[Batch ${batchId}] Full result:`, JSON.stringify(result, null, 2));
        return { data: [], log: logEntry };
      }

      const columns = result.manifest.schema.columns.map((c: any) => c.name.toLowerCase());
      const dataArray = result.result.data_array;

      console.log(`[Batch ${batchId}] Parsing ${dataArray.length} rows with columns: ${columns.join(', ')}`);

      const parsedData = dataArray.map((row: any[]) => {
        const rowObj: any = {};
        columns.forEach((col: string, idx: number) => {
          rowObj[col] = row[idx];
        });
        return rowObj;
      });

      logEntry.parsedCount = parsedData.length;
      console.log(`[Batch ${batchId}] ✅ Successfully parsed ${parsedData.length} rows`);
      
      return { data: parsedData, log: logEntry };

    } catch (error: any) {
      console.error(`[Batch ${batchId}] ❌ Error:`, error);
      if (!logEntry.error) {
        logEntry.error = error.message;
      }
      throw error; 
    }
  };

  const fetchColorMappings = useCallback(async (): Promise<Map<string, string>> => {
    const query = `
      SELECT CLRName, CLRCode
      FROM sportsdirect_sql.dbo.ap21_product
      WHERE CLRName IS NOT NULL 
        AND CLRCode IS NOT NULL
        AND CLRCode != "NULL"
        AND TRIM(CLRName) != ''
        AND TRIM(CLRCode) != ''
        AND LEN(TRIM(CLRName)) > 2
        AND LEN(TRIM(CLRCode)) >= 2
      GROUP BY CLRName, CLRCode
    `;

    try {
      const res = await fetchWithRetry(() => executeDatabricksQuery(query, -1, 'KEYS'));
      
      const colorMap = new Map<string, string>();
      
      res.data.forEach((row: any) => {
        const colorName = String(row.clrname || '').trim().toUpperCase();
        const colorCode = String(row.clrcode || '').trim().toUpperCase();
        
        if (colorName && 
            colorCode && 
            colorName.length > 2 && 
            colorCode.length >= 2 &&
            !/^[\s\-_\.\/\(\)]+$/.test(colorName) && 
            !/^[\s\-_\.\/\(\)0-9]+$/.test(colorCode) &&
            !colorName.includes('NULL') &&
            !colorCode.includes('NULL')) {
          
          if (!colorMap.has(colorName)) {
            colorMap.set(colorName, colorCode);
          }
        }
      });
      
      console.log(`Loaded ${colorMap.size} unique color mappings from database`);
      return colorMap;
      
    } catch (err: any) {
      console.error("Failed to fetch color mappings:", err);
      return new Map();
    }
  }, [token, host, path]);

  const findColorInFilename = useCallback((filename: string, colorMap: Map<string, string>): string | null => {
    const upperFilename = filename.toUpperCase();
    
    const sortedColors = Array.from(colorMap.entries()).sort((a, b) => b[0].length - a[0].length);
    
    for (const [colorName, colorCode] of sortedColors) {
      if (colorName.length < 3 || 
          ['SS', 'TEE', 'TOP', 'BOX', 'SET', 'CAMO', 'BLAZE', 'SHADOW', 
           'HONEYCOMB', 'LAUREL', 'WREATH', 'ULTRA', 'GLIDE', 'AERO'].includes(colorName)) {
        continue;
      }
      
      const escapedColorName = colorName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp(`\\b${escapedColorName.replace(/\s+/g, '[\\s_-]*')}\\b`, 'i');
      
      try {
        if (regex.test(upperFilename)) {
          console.log(`Found color in filename "${filename}": "${colorName}" -> ${colorCode}`);
          return colorCode;
        }
      } catch (regexError) {
        console.warn(`Skipping color "${colorName}" due to regex error`);
        continue;
      }
    }
    
    return null;
  }, []);

  const processEntries = useCallback(async (entries: {name: string, path: string}[]) => {
    if (!token) {
      setSyncError("Please enter your Databricks Access Token to process files.");
      return;
    }

    setSyncError(null);
    setDebugLogs([]);
    setStatus(FileStatus.PARSING);
    
    let accumulatedLogs: QueryLog[] = [];

    try {
      // 1. Parse all filenames to get candidates and REMOVE BRACKETS
      const extractedInfos = entries.map(entry => parseFilename(entry.name, entry.path));
      
      // 2. Collect all unique candidate codes to query (UPPERCASE + REMOVE BRACKETS)
      const allCandidates = new Set<string>();
      extractedInfos.forEach(info => {
        info.candidateCodes.forEach(code => {
          const cleaned = cleanProductCode(code.toUpperCase());
          if (cleaned) allCandidates.add(cleaned);
        });
      });

      const uniqueCandidatesList = Array.from(allCandidates);
      
      if (uniqueCandidatesList.length === 0) {
        setSyncError("Could not extract any potential product codes from the filenames.");
        setStatus(FileStatus.IDLE);
        return;
      }

      console.log(`Total unique candidates to validate: ${uniqueCandidatesList.length}`);

      // --- STAGE 1: Fetch ALL StyleCodes (excluding parent rows with NULL CLRCode) ---
      setStatus(FileStatus.QUERYING_KEYS);
      
      console.log('Fetching all StyleCodes from database (one-time query)...');
      
      const allStyleCodesQuery = `
        SELECT StyleCode
        FROM sportsdirect_sql.dbo.ap21_product
        WHERE CLRName IS NOT NULL 
        AND CLRCode IS NOT NULL
        AND CLRCode != "NULL"
        AND TRIM(COALESCE(CLRCode, '')) != ''
        GROUP BY StyleCode
      `;
      
      let allDbStyleCodes: Set<string>;
      
      try {
        const res = await fetchWithRetry(() => executeDatabricksQuery(allStyleCodesQuery, 0, 'KEYS'));
        
        allDbStyleCodes = new Set(
          res.data.map((row: any) => cleanProductCode(String(row.stylecode || '').toUpperCase().trim()))
        );
        
        accumulatedLogs.push(res.log);
        console.log(`Fetched ${allDbStyleCodes.size} StyleCodes from database (parent rows excluded)`);

      } catch (err: any) {
        console.error("Critical failure in Stage 1", err);
        setSyncError(`Failed to fetch StyleCodes (Stage 1): ${err.message}`);
        setStatus(FileStatus.ERROR);
        setDebugLogs(accumulatedLogs);
        return; 
      }
      
      // Validate which of our candidates exist in the database
      const foundKeys: { styleCode: string }[] = [];
      
      uniqueCandidatesList.forEach(candidate => {
        if (allDbStyleCodes.has(candidate)) {
          foundKeys.push({ styleCode: candidate });
        }
      });

      console.log(`Stage 1 Complete: Validated ${foundKeys.length} of ${uniqueCandidatesList.length} candidates exist in database`);

      // --- STAGE 2: Fetch Full Details for Verified Codes (ALL FIELDS, excluding parent rows) ---
      setStatus(FileStatus.PROCESSING);
      
      const verifiedStyleCodes = new Set<string>();
      foundKeys.forEach(k => verifiedStyleCodes.add(k.styleCode));

      const styleCodesToFetch = Array.from(verifiedStyleCodes);
      let fetchedRecords: ERPRecord[] = [];

      if (styleCodesToFetch.length > 0) {
        setStatus(FileStatus.QUERYING_DETAILS);
        
        const detailBatches = chunkArray(styleCodesToFetch, LARGE_BATCH_SIZE);
        
        for (let i = 0; i < detailBatches.length; i++) {
          const chunk = detailBatches[i];
          const safeCodes = chunk.map(c => c.replace(/'/g, "''").trim());
          const codeList = safeCodes.map(c => `'${c}'`).join(',');
          
          const query = `
            SELECT
              Name,
              StyleBrand,
              StyleCategory,
              CLRIDX,
              CLRCode,
              CLRName,
              StyleGender,
              EndUse,
              Model,
              StyleRange,
              SubCategory,
              StyleProdType,
              StyleCode,
              StyleColour,
              StyleSubRange,
              ProductAgeGroup,
              STYLEIDX
            FROM sportsdirect_sql.dbo.ap21_product
            WHERE UPPER(StyleCode) IN (${safeCodes.map(c => `UPPER('${c}')`).join(',')})
            AND CLRName IS NOT NULL
            AND CLRCode IS NOT NULL
            AND CLRCode != "NULL"
            AND TRIM(COALESCE(CLRCode, '')) != ''
          `;

          try {
            const res = await fetchWithRetry(() => executeDatabricksQuery(query, i + 1, 'DETAILS'));
            
            const records = res.data.map((row: any) => ({
              // Core fields
              productCode: cleanProductCode(String(row.stylecode || '').toUpperCase().trim()),
              styleCode: cleanProductCode(String(row.stylecode || '').toUpperCase().trim()),
              clrCode: cleanProductCode(String(row.clrcode || '').toUpperCase().trim()),

              // Required fields
              name: String(row.name || '').trim(),
              brand: String(row.stylebrand || '').toUpperCase().trim(),
              category: String(row.stylecategory || '').trim(),
              clridx: String(row.clridx || '').trim(),
              colourCode: cleanProductCode(String(row.clrcode || '').toUpperCase().trim()),
              gender: String(row.stylegender || '').trim(),
              productEndUse: String(row.enduse || '').trim(),
              productModel: String(row.model || '').trim(),
              productRange: String(row.stylerange || '').trim(),
              productSubCategory: String(row.subcategory || '').trim(),
              productType: String(row.styleprodtype || '').trim(),
              styleColour: String(row.stylecolour || '').trim(),
              styleSubRange: String(row.stylesubrange || '').trim(),
              ageGroup: String(row.productagegroup || '').trim(),
              clrName: String(row.clrname || '').trim(),
              styleIdx: String(row.styleidx || '').trim(),

              // Legacy compatibility
              styleCategory: String(row.stylecategory || '').trim(),
              productName: String(row.name || '').trim()
            }));
            
            fetchedRecords = [...fetchedRecords, ...records];
            accumulatedLogs.push(res.log);

            console.log(`Stage 2 Batch ${i + 1}/${detailBatches.length}: Fetched ${records.length} detailed records (parent rows excluded)`);

          } catch (err: any) {
            console.error("Critical failure in Stage 2", err);
            setSyncError(`Failed to fetch details (Stage 2): ${err.message}`);
            setStatus(FileStatus.ERROR);
            setDebugLogs(accumulatedLogs);
            return;
          }
        }
        
        console.log(`Stage 2 Complete: Fetched ${fetchedRecords.length} total detailed records`);
      }

      // --- STAGE 3: Initial Matching (LONGEST FIRST + MULTI-COLOR VALIDATION) ---
      const erpData = fetchedRecords;

      // Pre-compute: group records by StyleCode and count distinct CLRCodes
      const styleCodeRecordsMap = new Map<string, ERPRecord[]>();
      erpData.forEach(record => {
        const sc = record.styleCode;
        if (!styleCodeRecordsMap.has(sc)) {
          styleCodeRecordsMap.set(sc, []);
        }
        styleCodeRecordsMap.get(sc)!.push(record);
      });

      const styleCodeDistinctColorCount = new Map<string, number>();
      styleCodeRecordsMap.forEach((records, sc) => {
        const distinctColors = new Set(records.map(r => r.clrCode).filter(c => c && c !== 'NULL')).size;
        styleCodeDistinctColorCount.set(sc, distinctColors);
      });

      console.log(`Color counts computed for ${styleCodeDistinctColorCount.size} StyleCodes`);
      
      // Log which StyleCodes have >2 colors for debugging
      styleCodeDistinctColorCount.forEach((count, sc) => {
        if (count > 2) {
          const records = styleCodeRecordsMap.get(sc) || [];
          const colors = [...new Set(records.map(r => r.clrCode))];
          console.log(`[Multi-color] StyleCode ${sc} has ${count} distinct CLRCodes: ${colors.join(', ')}`);
        }
      });

      // Fetch color name->code mappings for name-based resolution (needed for multi-color matching)
      const colorMap = await fetchColorMappings();

      const allErpCodes = erpData.flatMap(d => [d.styleCode, d.clrCode]).filter(Boolean);

      let matchedResults: MatchResult[] = extractedInfos.map(info => {
        // Clean and sort attempts (longest first, uppercase, no brackets)
        const attempts = info.candidateCodes
          .map(c => cleanProductCode(c.toUpperCase()))
          .filter(Boolean)
          .sort((a, b) => b.length - a.length);

        let directMatch: ERPRecord | undefined = undefined;
        let matchedAtStage: string | undefined = undefined;

        // Try direct match (longest codes first)
        for (const candidate of attempts) {
          const found = erpData.find(d => 
            d.styleCode === candidate || d.clrCode === candidate
          );
          if (found) {
            directMatch = found;
            matchedAtStage = candidate;
            break;
          }
        }
        
        if (directMatch && matchedAtStage) {
          const matchedStyleCode = directMatch.styleCode;
          const colorCount = styleCodeDistinctColorCount.get(matchedStyleCode) || 0;

          // --- MULTI-COLOR VALIDATION (>2 distinct CLRCodes for this StyleCode) ---
          if (colorCount > 2) {
            console.log(`[Multi-color] StyleCode ${matchedStyleCode} has ${colorCount} colors — validating color from filename "${info.fileName}"`);

            const recordsForStyle = styleCodeRecordsMap.get(matchedStyleCode) || [];
            const availableClrCodes = [...new Set(recordsForStyle.map(r => r.clrCode))];

            // 1. Extract color hint from filename (part right after the style code)
            const colorHint = extractColorHintFromFilename(info.fileName, matchedStyleCode);
            console.log(`[Multi-color] Color hint from filename: ${colorHint || 'NONE'}`);

            if (colorHint) {
              // 2a. Try direct CLRCode match (e.g., "BLK-PKY" in filename matches CLRCode "BLK-PKY")
              const directColorMatch = recordsForStyle.find(r => r.clrCode === colorHint);
              if (directColorMatch) {
                console.log(`[Multi-color] ✅ Direct CLRCode match: ${colorHint}`);
                return {
                  ...info,
                  productCode: `${matchedStyleCode} / ${colorHint}`,
                  attempts,
                  isMatch: true,
                  isFuzzy: false,
                  ...directColorMatch,
                  status: 'SUCCESS' as const,
                  reason: `Multi-color match: StyleCode ${matchedStyleCode} + CLRCode ${colorHint} (${colorCount} colours available)`
                };
              }

              // 2b. Try color NAME lookup (e.g., "WHITE" in filename -> CLRCode "WHT" via color map)
              const upperHint = colorHint.toUpperCase();
              if (colorMap.has(upperHint)) {
                const mappedCode = colorMap.get(upperHint)!;
                const nameMatch = recordsForStyle.find(r => r.clrCode === mappedCode);
                if (nameMatch) {
                  console.log(`[Multi-color] ✅ Color name match: "${upperHint}" -> CLRCode "${mappedCode}"`);
                  return {
                    ...info,
                    productCode: `${matchedStyleCode} / ${mappedCode}`,
                    attempts,
                    isMatch: true,
                    isFuzzy: false,
                    ...nameMatch,
                    status: 'SUCCESS' as const,
                    reason: `Multi-color match: StyleCode ${matchedStyleCode} + colour "${upperHint}" -> CLRCode ${mappedCode} (${colorCount} colours available)`
                  };
                }
              }

              // 2c. Try partial/fuzzy CLRCode match (e.g., "WHT" in filename, CLRCode contains "WHT")
              const partialColorMatch = recordsForStyle.find(r =>
                r.clrCode.startsWith(colorHint) || r.clrCode.includes(colorHint)
              );
              if (partialColorMatch) {
                console.log(`[Multi-color] ⚠️ Partial CLRCode match: "${colorHint}" found in "${partialColorMatch.clrCode}"`);
                return {
                  ...info,
                  productCode: `${matchedStyleCode} / ${partialColorMatch.clrCode}`,
                  attempts,
                  isMatch: true,
                  isFuzzy: true,
                  fuzzyMatchCode: partialColorMatch.clrCode,
                  ...partialColorMatch,
                  status: 'FUZZY' as const,
                  reason: `Multi-color partial: "${colorHint}" ~ CLRCode "${partialColorMatch.clrCode}" (${colorCount} colours available)`
                };
              }

              // 2d. Try reverse lookup — check if colorHint IS already a known CLRCode abbreviation
              for (const [cName, cCode] of colorMap.entries()) {
                if (cCode === colorHint) {
                  const reverseMatch = recordsForStyle.find(r => r.clrCode === colorHint);
                  if (reverseMatch) {
                    console.log(`[Multi-color] ✅ Reverse color match: ${colorHint}`);
                    return {
                      ...info,
                      productCode: `${matchedStyleCode} / ${colorHint}`,
                      attempts,
                      isMatch: true,
                      isFuzzy: false,
                      ...reverseMatch,
                      status: 'SUCCESS' as const,
                      reason: `Multi-color match: StyleCode ${matchedStyleCode} + CLRCode ${colorHint} (${colorCount} colours available)`
                    };
                  }
                  break;
                }
              }

              // 2e. Also try: does the hint match any CLRCode when we check for the hint
              //     being a substring at the start of a compound code like "WHT-BLK"?
              const startsWithMatch = recordsForStyle.find(r => {
                const firstPart = r.clrCode.split('-')[0];
                return firstPart === colorHint;
              });
              if (startsWithMatch) {
                console.log(`[Multi-color] ⚠️ First-part CLRCode match: "${colorHint}" is start of "${startsWithMatch.clrCode}"`);
                return {
                  ...info,
                  productCode: `${matchedStyleCode} / ${startsWithMatch.clrCode}`,
                  attempts,
                  isMatch: true,
                  isFuzzy: true,
                  fuzzyMatchCode: startsWithMatch.clrCode,
                  ...startsWithMatch,
                  status: 'FUZZY' as const,
                  reason: `Multi-color partial: "${colorHint}" matches first part of CLRCode "${startsWithMatch.clrCode}" (${colorCount} colours available)`
                };
              }

              // Color hint found but couldn't resolve to a specific CLRCode — flag for manual check
              console.log(`[Multi-color] ❌ Could not resolve color "${colorHint}" to any CLRCode. Available: ${availableClrCodes.join(', ')}`);
              return {
                ...info,
                productCode: matchedAtStage,
                attempts,
                isMatch: true,
                isFuzzy: false,
                ...directMatch,
                colorVariantCount: colorCount,
                status: 'Multi Colour AP21 - Cant Find' as const,
                reason: `StyleCode ${matchedStyleCode} has ${colorCount} colours. Colour hint "${colorHint}" not resolved. Available CLRCodes: ${availableClrCodes.join(', ')}`
              };
            }

            // No color hint extractable from filename — flag for manual check
            console.log(`[Multi-color] ❌ No color hint in filename. Available CLRCodes: ${availableClrCodes.join(', ')}`);
            return {
              ...info,
              productCode: matchedAtStage,
              attempts,
              isMatch: true,
              isFuzzy: false,
              ...directMatch,
              colorVariantCount: colorCount,
              status: 'Multi Colour AP21 - No Reference' as const,
              reason: `StyleCode ${matchedStyleCode} has ${colorCount} colours but no colour detected in filename. Available CLRCodes: ${availableClrCodes.join(', ')}`
            };
          }

          // --- STANDARD MATCH (≤2 colours, no multi-color validation needed) ---
          const filenameColorCount = countColorComponents(attempts[0]);
          const matchedColorCount = countColorComponents(matchedAtStage);
          const needsCheck = filenameColorCount > matchedColorCount && matchedColorCount > 0;
          
          return {
            ...info,
            productCode: matchedAtStage,
            attempts,
            isMatch: true,
            isFuzzy: false,
            ...directMatch,
            colorVariantCount: colorCount,
            status: needsCheck ? 'Multi Colour in Name' as const : 'SUCCESS' as const,
            reason: needsCheck
              ? `Partial match: ${matchedAtStage} (filename suggests more colors)`
              : `Direct match: ${matchedAtStage}`
          };
        }

        // Try fuzzy match
        let bestFuzzy: { code: string; distance: number; source: string } | null = null;
        for (const candidate of attempts) {
          const fuzzy = findBestFuzzyMatch(candidate, allErpCodes, 2);
          if (fuzzy && (!bestFuzzy || fuzzy.distance < bestFuzzy.distance)) {
            bestFuzzy = { ...fuzzy, source: candidate };
          }
        }

        if (bestFuzzy) {
          const fuzzyRecord = erpData.find(d => d.styleCode === bestFuzzy!.code || d.clrCode === bestFuzzy!.code);
          
          if (fuzzyRecord) {
            const filenameColorCount = countColorComponents(attempts[0]);
            const matchedColorCount = countColorComponents(bestFuzzy.code);
            const needsCheck = filenameColorCount > matchedColorCount && matchedColorCount > 0;
            
            return {
              ...info,
              attempts,
              isMatch: true,
              isFuzzy: true,
              fuzzyMatchCode: bestFuzzy.code,
              ...fuzzyRecord,
              colorVariantCount: styleCodeDistinctColorCount.get(fuzzyRecord.styleCode) || 0,
              status: needsCheck ? 'Multi Colour in Name' as const : 'FUZZY' as const,
              reason: needsCheck
                ? `Fuzzy partial match: "${bestFuzzy.source}" to "${bestFuzzy.code}" (filename suggests more colors)`
                : `Fuzzy matched "${bestFuzzy.source}" to "${bestFuzzy.code}"`
            };
          }
        }

        // No match yet
        return {
          ...info,
          attempts,
          isMatch: false,
          isFuzzy: false,
          status: 'FAILURE' as const,
          reason: `No match found in ERP for: ${attempts.join(', ')}`
        };
      });

      // Show initial results
      setDebugLogs(accumulatedLogs);
      setResults(matchedResults);
      setStatus(FileStatus.PROCESSING);

      console.log(`Initial validation complete: ${matchedResults.filter(r => r.status === 'SUCCESS').length} success, ${matchedResults.filter(r => r.status === 'FUZZY').length} fuzzy, ${matchedResults.filter(r => r.status === 'Multi Colour AP21 - Cant Find' || r.status === 'Multi Colour AP21 - No Reference' || r.status === 'Multi Colour in Name').length} checked, ${matchedResults.filter(r => r.status === 'FAILURE').length} failed`);

      // --- STAGE 4: Color-Based Fallback (ONLY for failures) ---
      const failedMatches = matchedResults.filter(r => r.status === 'FAILURE');
      
      if (failedMatches.length > 0) {
        console.log(`\n=== COLOR FALLBACK STAGE ===`);
        console.log(`Attempting color-based matching for ${failedMatches.length} failed files...`);
        
        // Reuse the colorMap we already fetched in Stage 3
        if (colorMap.size > 0) {
          const colorCandidates = new Set<string>();
          const failureEntries = failedMatches.map(failedMatch => {
            return entries.find(e => e.name === failedMatch.fileName);
          }).filter(Boolean);
          
          const colorEnhancedInfos = failureEntries.map(entry => {
            const baseInfo = parseFilename(entry!.name, entry!.path);
            const detectedColor = findColorInFilename(entry!.name, colorMap);
            
            if (detectedColor && baseInfo.candidateCodes.length > 0) {
              const baseCode = cleanProductCode(baseInfo.candidateCodes[0].split('-')[0].split('_')[0].toUpperCase());
              const colorVariant = `${baseCode}-${detectedColor}`;
              colorCandidates.add(colorVariant);
              
              return {
                ...baseInfo,
                candidateCodes: [colorVariant, ...baseInfo.candidateCodes.map(c => cleanProductCode(c.toUpperCase()))].filter(Boolean),
                fileName: entry!.name
              };
            }
            return null;
          }).filter(Boolean);
          
          if (colorCandidates.size > 0) {
            console.log(`Generated ${colorCandidates.size} color-enhanced candidates`);
            
            const colorCandidatesList = Array.from(colorCandidates);
            let colorFoundKeys: { styleCode: string }[] = [];
            
            colorCandidatesList.forEach(candidate => {
              if (allDbStyleCodes.has(candidate)) {
                colorFoundKeys.push({ styleCode: candidate });
              }
            });
            
            console.log(`Color stage found ${colorFoundKeys.length} verified StyleCodes`);
            
            if (colorFoundKeys.length > 0) {
              const colorVerifiedStyleCodes = new Set<string>();
              colorFoundKeys.forEach(k => colorVerifiedStyleCodes.add(k.styleCode));
              
              const colorStyleCodesToFetch = Array.from(colorVerifiedStyleCodes);
              let colorFetchedRecords: ERPRecord[] = [];
              
              const colorDetailBatches = chunkArray(colorStyleCodesToFetch, LARGE_BATCH_SIZE);
              
              for (let i = 0; i < colorDetailBatches.length; i++) {
                const chunk = colorDetailBatches[i];
                const safeCodes = chunk.map(c => c.replace(/'/g, "''").trim());
                
                const query = `
                  SELECT
                    Name,
                    StyleBrand,
                    StyleCategory,
                    CLRIDX,
                    CLRCode,
                    CLRName,
                    StyleGender,
                    EndUse,
                    Model,
                    StyleRange,
                    SubCategory,
                    StyleProdType,
                    StyleCode,
                    StyleColour,
                    StyleSubRange,
                    ProductAgeGroup,
                    STYLEIDX
                  FROM sportsdirect_sql.dbo.ap21_product
                  WHERE UPPER(StyleCode) IN (${safeCodes.map(c => `UPPER('${c}')`).join(',')})
                  AND CLRName IS NOT NULL
                  AND CLRCode IS NOT NULL
                  AND CLRCode != "NULL"
                  AND TRIM(COALESCE(CLRCode, '')) != ''
                `;
                
                try {
                  const res = await fetchWithRetry(() => 
                    executeDatabricksQuery(query, 2000 + i, 'DETAILS')
                  );
                  
                  const records = res.data.map((row: any) => ({
                    productCode: cleanProductCode(String(row.stylecode || '').toUpperCase().trim()),
                    styleCode: cleanProductCode(String(row.stylecode || '').toUpperCase().trim()),
                    clrCode: cleanProductCode(String(row.clrcode || '').toUpperCase().trim()),
                    name: String(row.name || '').trim(),
                    brand: String(row.stylebrand || '').toUpperCase().trim(),
                    category: String(row.stylecategory || '').trim(),
                    clridx: String(row.clridx || '').trim(),
                    colourCode: cleanProductCode(String(row.clrcode || '').toUpperCase().trim()),
                    gender: String(row.stylegender || '').trim(),
                    productEndUse: String(row.enduse || '').trim(),
                    productModel: String(row.model || '').trim(),
                    productRange: String(row.stylerange || '').trim(),
                    productSubCategory: String(row.subcategory || '').trim(),
                    productType: String(row.styleprodtype || '').trim(),
                    styleColour: String(row.stylecolour || '').trim(),
                    styleSubRange: String(row.stylesubrange || '').trim(),
                    ageGroup: String(row.productagegroup || '').trim(),
                    clrName: String(row.clrname || '').trim(),
                    styleIdx: String(row.styleidx || '').trim(),
                    styleCategory: String(row.stylecategory || '').trim(),
                    productName: String(row.name || '').trim()
                  }));
                  
                  colorFetchedRecords = [...colorFetchedRecords, ...records];
                  accumulatedLogs.push(res.log);
                  
                } catch (err: any) {
                  console.warn("Color detail fetch failed:", err.message);
                }
              }
              
              console.log(`Color stage fetched ${colorFetchedRecords.length} detailed records`);
              
              matchedResults = matchedResults.map(result => {
                if (result.status !== 'FAILURE') return result;
                
                const colorInfo = colorEnhancedInfos.find((ci: any) => ci?.fileName === result.fileName);
                if (!colorInfo) return result;
                
                const sortedColorCandidates = colorInfo.candidateCodes.sort((a, b) => b.length - a.length);
                
                for (const candidate of sortedColorCandidates) {
                  const found = colorFetchedRecords.find(d => 
                    d.styleCode === candidate || d.clrCode === candidate
                  );
                  if (found) {
                    const filenameColorCount = countColorComponents(sortedColorCandidates[0]);
                    const matchedColorCount = countColorComponents(candidate);
                    const needsCheck = filenameColorCount > matchedColorCount && matchedColorCount > 0;
                    
                    return {
                      ...result,
                      productCode: candidate,
                      attempts: sortedColorCandidates,
                      isMatch: true,
                      isFuzzy: false,
                      ...found,
                      colorVariantCount: styleCodeDistinctColorCount.get(found.styleCode) || 0,
                      status: needsCheck ? 'Multi Colour in Name' as const : 'SUCCESS' as const,
                      reason: needsCheck
                        ? `Color-based partial match: ${candidate} (filename suggests more colors)`
                        : `Color-based match: ${candidate}`
                    };
                  }
                }
                
                const colorErpCodes = colorFetchedRecords.flatMap(d => [d.styleCode, d.clrCode]).filter(Boolean);
                let bestFuzzy: { code: string; distance: number; source: string } | null = null;
                
                for (const candidate of sortedColorCandidates) {
                  const fuzzy = findBestFuzzyMatch(candidate, colorErpCodes, 2);
                  if (fuzzy && (!bestFuzzy || fuzzy.distance < bestFuzzy.distance)) {
                    bestFuzzy = { ...fuzzy, source: candidate };
                  }
                }
                
                if (bestFuzzy) {
                  const fuzzyRecord = colorFetchedRecords.find(d => 
                    d.styleCode === bestFuzzy!.code || d.clrCode === bestFuzzy!.code
                  );
                  if (fuzzyRecord) {
                    const filenameColorCount = countColorComponents(sortedColorCandidates[0]);
                    const matchedColorCount = countColorComponents(bestFuzzy.code);
                    const needsCheck = filenameColorCount > matchedColorCount && matchedColorCount > 0;
                    
                    return {
                      ...result,
                      attempts: sortedColorCandidates,
                      isMatch: true,
                      isFuzzy: true,
                      fuzzyMatchCode: bestFuzzy.code,
                      ...fuzzyRecord,
                      colorVariantCount: styleCodeDistinctColorCount.get(fuzzyRecord.styleCode) || 0,
                      status: needsCheck ? 'Multi Colour in Name' as const : 'FUZZY' as const,
                      reason: needsCheck
                        ? `Color-based fuzzy partial match: "${bestFuzzy.source}" to "${bestFuzzy.code}" (filename suggests more colors)`
                        : `Color-based fuzzy match: "${bestFuzzy.source}" to "${bestFuzzy.code}"`
                    };
                  }
                }
                
                return result;
              });
              
              const newSuccesses = matchedResults.filter(r =>
                (r.status === 'SUCCESS' || r.status === 'Multi Colour in Name') && r.reason?.includes('Color-based')
              ).length;
              
              console.log(`Color fallback recovered ${newSuccesses} additional matches`);
            }
          }
        }
      }

      setDebugLogs(accumulatedLogs);
      setResults(matchedResults);
      setStatus(FileStatus.COMPLETE);

    } catch (e: any) {
      console.error(e);
      setSyncError("Unexpected error: " + e.message);
      setStatus(FileStatus.ERROR);
      setDebugLogs(accumulatedLogs);
    }
  }, [token, host, path, fetchColorMappings, findColorInFilename]);

  const handleFileProcessing = useCallback((files: File[]) => {
    if (files.length === 0) return;
    
    // Filter out folders and non-file entries (size === 0 with no extension is likely a folder)
    const validFiles = Array.from(files).filter(f => {
      // Skip entries with no name
      if (!f.name) return false;
      // Skip hidden files (starting with .)
      if (f.name.startsWith('.')) return false;
      // Skip system files
      if (f.name === 'Thumbs.db' || f.name === '.DS_Store' || f.name === 'desktop.ini') return false;
      // Skip zero-byte files with no extension (likely folder entries)
      if (f.size === 0 && !f.name.includes('.')) return false;
      return true;
    });

    if (validFiles.length === 0) {
      alert("No valid files detected in your selection.");
      return;
    }

    const csvFile = validFiles.find(f => f.name.toLowerCase().endsWith('.csv'));
    
    if (csvFile) {
      const reader = new FileReader();
      reader.onload = (e) => {
        const text = e.target?.result as string;
        const listData = parseCSV(text);
        const entries = listData.map(row => ({
          name: row['file name'] || row.filename || row.name || row['product code'] || 'Unknown',
          path: `CSV Upload / ${row.brand || 'No Brand'}`
        }));
        processEntries(entries);
      };
      reader.onerror = () => {
        alert("Failed to read CSV file.");
      };
      reader.readAsText(csvFile);
    } else {
      // Only process image files — skip everything else (folders, PDFs, docs, etc.)
      const imageExtensions = /\.(jpg|jpeg|png|webp|gif|bmp|tiff|tif|svg|avif)$/i;
      const entries = validFiles
        .filter(f => f.type.startsWith('image/') || imageExtensions.test(f.name))
        .map(f => ({ name: f.name, path: f.webkitRelativePath || f.name }));
      
      const skippedCount = validFiles.length - entries.length;
      if (skippedCount > 0) {
        console.log(`Skipped ${skippedCount} non-image files/folders`);
      }

      if (entries.length === 0) {
        alert("No valid image files detected in your selection. Only image files (JPG, PNG, WebP, GIF, etc.) and CSV files are supported.");
        return;
      }
      
      console.log(`Processing ${entries.length} image files (skipped ${skippedCount} non-image items)`);
      processEntries(entries);
    }
  }, [processEntries]);

  const handleReset = () => {
    setResults([]);
    setSyncError(null);
    setDebugLogs([]);
    setStatus(FileStatus.IDLE);
  };

  const downloadCSV = (data: string, filename: string) => {
    const blob = new Blob([data], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', filename);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const exportDebugLogs = () => {
    const jsonStr = JSON.stringify(debugLogs, null, 2);
    const blob = new Blob([jsonStr], { type: 'application/json' });
    const link = document.createElement('a');
    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', 'databricks_debug_logs.json');
    link.click();
  };

  const exportResults = () => {
    const successHeader = "File Name,ID,Status,Product Code,Colour Code,Name,Product Range,Brand,Category,Gender,Product Type,Sub Range,Style Colour,Product End Use,Product Model,Product Sub Category,Clr IDX,AgeGroup,CLRName,STYLEIDX,Colour Variant Count,Category Success,Category,Angle Success,Angle Assigned\n";
    const failureHeader = "File Name,Brand Hint,Attempted Codes,Reason\n";

    const successRows = results
      .filter(r => r.status === 'SUCCESS' || r.status === 'Multi Colour AP21 - Cant Find' || r.status === 'Multi Colour AP21 - No Reference' || r.status === 'Multi Colour in Name')
      .map(r => {
        const id = r.fileName.replace(/\.[^/.]+$/, '');
        const productCode = r.isFuzzy ? r.fuzzyMatchCode : r.productCode;
        return `"${r.fileName}","${id}","${r.status}","${productCode || ''}","${r.colourCode || ''}","${r.name || ''}","${r.productRange || ''}","${r.brand || ''}","${r.category || ''}","${r.gender || ''}","${r.productType || ''}","${r.styleSubRange || ''}","${r.styleColour || ''}","${r.productEndUse || ''}","${r.productModel || ''}","${r.productSubCategory || ''}","${r.clridx || ''}","${r.ageGroup || ''}","${r.clrName || ''}","${r.styleIdx || ''}","${r.colorVariantCount ?? 0}","","","",""`;
      })
      .join('\n');

    const failureRows = results
      .filter(r => r.status === 'FAILURE' || r.status === 'FUZZY')
      .map(r => `"${r.fileName}","${r.brandHint}","${r.attempts.join(' | ')}","${r.reason}"`)
      .join('\n');

    if (successRows) {
      downloadCSV(successHeader + successRows, "validation_success.csv");
    }
    if (failureRows) {
      downloadCSV(failureHeader + failureRows, "validation_failed.csv");
    }
  };

  const stats = {
    total: results.length,
    success: results.filter(r => r.status === 'SUCCESS').length,
    fuzzy: results.filter(r => r.status === 'FUZZY').length,
    checked: results.filter(r => r.status === 'Multi Colour AP21 - Cant Find' || r.status === 'Multi Colour AP21 - No Reference' || r.status === 'Multi Colour in Name').length,
    failure: results.filter(r => r.status === 'FAILURE').length
  };

  const isWorking = [
    FileStatus.PARSING, 
    FileStatus.QUERYING_KEYS, 
    FileStatus.QUERYING_DETAILS, 
    FileStatus.PROCESSING
  ].includes(status);

  return (
    <div className="min-h-screen bg-slate-50 p-4 md:p-10 font-sans">
      <div className="max-w-7xl mx-auto space-y-8">
        <header className="flex flex-col md:flex-row md:items-center justify-between gap-6">
          <div className="space-y-1">
            <h1 className="text-3xl font-bold text-slate-900 tracking-tight">ERP Filename Validator</h1>
            <p className="text-slate-500">Auto-query Databricks for Product Code, Name, and Category verification.</p>
          </div>
          
          <div className="flex flex-wrap items-center gap-3">
            {results.length > 0 && (
              <>
                <button 
                  onClick={handleReset}
                  className="px-4 py-2 bg-white border border-slate-200 text-slate-600 rounded-xl font-semibold text-sm hover:bg-slate-50 transition-colors shadow-sm flex items-center gap-2"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                  </svg>
                  Reset All
                </button>
                <button 
                  onClick={exportDebugLogs}
                  className="px-4 py-2 bg-slate-800 text-white rounded-xl font-semibold text-sm hover:bg-slate-900 transition-all shadow-md flex items-center gap-2"
                  title="Download raw SQL request/response logs"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" />
                  </svg>
                  Debug Logs
                </button>
                <button 
                  onClick={exportResults}
                  className="px-4 py-2 bg-indigo-600 text-white rounded-xl font-semibold text-sm hover:bg-indigo-700 transition-all shadow-md flex items-center gap-2"
                >
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                  </svg>
                  Export CSVs
                </button>
              </>
            )}
            <div className="flex items-center gap-2 px-3 py-1.5 bg-white border border-slate-200 rounded-xl shadow-sm">
              <div className={`h-2 w-2 rounded-full ${isWorking ? 'bg-indigo-500 animate-pulse' : (status === FileStatus.COMPLETE ? 'bg-green-500' : 'bg-slate-300')}`} />
              <span className="text-[10px] font-black text-slate-700 uppercase tracking-widest">
                {status}
              </span>
            </div>
          </div>
        </header>

        <section className="grid grid-cols-1 md:grid-cols-2 gap-6">
          <div className={`bg-white p-6 rounded-2xl shadow-sm border transition-all duration-300 ${syncError ? 'border-red-200' : 'border-slate-200'} space-y-4`}>
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                <div className="p-2 rounded-lg bg-slate-100 text-slate-600">
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" />
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" />
                  </svg>
                </div>
                <h2 className="font-bold text-lg text-slate-800">1. Connection Settings</h2>
              </div>
            </div>

            <div className="space-y-3">
              <div className="grid grid-cols-1 gap-3">
                <div className="space-y-1">
                  <label className="text-[10px] uppercase font-bold text-slate-400 tracking-tight ml-1">Access Token</label>
                  <input 
                    type="password"
                    placeholder="dapi..."
                    className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-500 outline-none"
                    value={token}
                    onChange={(e) => setToken(e.target.value)}
                  />
                </div>
                <div className="grid grid-cols-2 gap-2">
                  <div className="space-y-1">
                    <label className="text-[10px] uppercase font-bold text-slate-400 tracking-tight ml-1">Server Host</label>
                    <input 
                      type="text"
                      className="w-full px-3 py-2 text-[11px] font-mono border border-slate-200 rounded-lg bg-slate-50"
                      value={host}
                      onChange={(e) => setHost(e.target.value)}
                    />
                  </div>
                  <div className="space-y-1">
                    <label className="text-[10px] uppercase font-bold text-slate-400 tracking-tight ml-1">HTTP Path</label>
                    <input 
                      type="text"
                      className="w-full px-3 py-2 text-[11px] font-mono border border-slate-200 rounded-lg bg-slate-50"
                      value={path}
                      onChange={(e) => setPath(e.target.value)}
                    />
                  </div>
                </div>
              </div>

              {syncError && (
                <div className="p-3 bg-red-50 border border-red-100 rounded-xl flex items-start gap-3 animate-in fade-in slide-in-from-top-1">
                  <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-red-500 shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <div className="flex flex-col">
                    <p className="text-xs font-bold text-red-700">Config Error</p>
                    <p className="text-[11px] text-red-600 leading-tight">{syncError}</p>
                  </div>
                </div>
              )}
              
              <div className="text-[11px] text-slate-400 italic bg-slate-50 p-2 rounded border border-slate-100">
                Connection details are used only when you drop files to validate.
              </div>
            </div>
          </div>

          <div className="bg-white p-6 rounded-2xl shadow-sm border border-slate-200 space-y-4">
            <div className="flex items-center gap-2 mb-2">
              <div className="bg-slate-100 p-2 rounded-lg text-slate-600">
                <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
                </svg>
              </div>
              <h2 className="font-bold text-lg text-slate-800">2. Drop Files to Query</h2>
            </div>
            <FileUploadZone 
              label="Upload Images/Folders" 
              onFilesSelected={handleFileProcessing} 
              isFolder={true}
              accept="image/*,.csv"
            />
            {isWorking && (
              <div className="text-center py-2 animate-pulse">
                 <p className="text-xs font-bold text-indigo-600">
                    {status === FileStatus.QUERYING_KEYS ? 'Verifying codes in ERP...' : 
                     status === FileStatus.QUERYING_DETAILS ? 'Fetching product attributes...' : 
                     'Processing matches...'}
                 </p>
              </div>
            )}
          </div>
        </section>

        {results.length > 0 && (
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4 animate-in fade-in slide-in-from-bottom-2 duration-500">
            {[
              { label: 'Total', value: stats.total, color: 'indigo' },
              { label: 'Success', value: stats.success, color: 'green' },
              { label: 'Fuzzy', value: stats.fuzzy, color: 'orange' },
              { label: 'Checked', value: stats.checked, color: 'yellow' },
              { label: 'Fail', value: stats.failure, color: 'red' }
            ].map(stat => (
              <div key={stat.label} className={`bg-white p-4 rounded-xl border-l-4 border-${stat.color}-500 shadow-sm ring-1 ring-slate-200`}>
                <p className="text-slate-500 text-[10px] font-bold uppercase tracking-tight">{stat.label}</p>
                <p className="text-2xl font-black text-slate-800">{stat.value}</p>
              </div>
            ))}
          </div>
        )}

        <main className="space-y-4 pb-20">
          <ResultsTable results={results} />
        </main>
      </div>
    </div>
  );
};

export default App;