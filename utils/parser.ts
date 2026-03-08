
import { BRANDS_WITH_DASH_RULE, DELIMITERS, COLOR_MAPPINGS, NOISE_WORDS, STRIP_PREFIXES } from '../constants';
import { ExtractedInfo } from '../types';

export const normalizeColor = (code: string): string => {
  const upper = code.toUpperCase();
  return COLOR_MAPPINGS[upper] || upper;
};

const isLikelyColor = (token: string): boolean => {
  const upper = token.toUpperCase();
  // Colors are usually 3 digits or known short strings like BLK, OAT, WHT, BCKT
  return (upper.length === 3) || !!COLOR_MAPPINGS[upper] || upper === 'MULTI' || /^[A-Z]{3,4}$/.test(upper);
};

const isDescriptiveWord = (token: string): boolean => {
  const upper = token.toUpperCase();
  // Only filter out words explicitly in our noise word list.
  if (NOISE_WORDS.has(upper)) return true;
  return false;
};

export const parseFilename = (inputName: string, path: string = ""): ExtractedInfo => {
  // Strip known prefixes (e.g. "MRLW-") that are not relevant for matching
  let cleanedName = inputName;
  for (const prefix of STRIP_PREFIXES) {
    const regex = new RegExp(`^${prefix}-`, 'i');
    if (regex.test(cleanedName)) {
      cleanedName = cleanedName.replace(regex, '');
      break;
    }
  }

  const nameNoExt = cleanedName.includes('.')
    ? cleanedName.substring(0, cleanedName.lastIndexOf('.'))
    : cleanedName;

  // Extract brand hint from directory structure if available
  const brandMatch = path.match(/Imagery - ([^\/\\]+)/i);
  const brandHint = brandMatch ? brandMatch[1].toUpperCase() : "UNKNOWN";

  // Split by multiple delimiters: underscore, dash, dot, space
  const allTokens = nameNoExt.split(DELIMITERS).filter(t => t.length > 0);
  
  // Filter out the brand name if it's a token, and general noise
  const validTokens = allTokens.filter(t => {
    const upper = t.toUpperCase();
    return upper !== brandHint && !isDescriptiveWord(t);
  });

  const candidates: string[] = [];
  let colorCode = "";

  /**
   * 3-Stage Progressive Candidate Generation
   * We want to try several combinations to find the ERP code:
   * 1. S1-S2-S3 (Full detailed)
   * 2. S1-S2 (Style-Color)
   * 3. S1 (Base Style)
   * 4. S1S2 (Concatenated Style-Color)
   * 5. S1S2S3 (Concatenated Full)
   */
  if (validTokens.length > 0) {
    const s1 = validTokens[0].toUpperCase();
    
    // Level 1: Single Segment
    candidates.push(s1);

    if (validTokens.length > 1) {
      const s2 = validTokens[1].toUpperCase();
      
      // Level 2: Two Segments
      candidates.push(`${s1}-${s2}`);
      candidates.push(`${s1}${s2}`);

      if (validTokens.length > 2) {
        const s3 = validTokens[2].toUpperCase();
        
        // Level 3: Three Segments
        candidates.push(`${s1}-${s2}-${s3}`);
        candidates.push(`${s1}${s2}${s3}`);
      }
    }
  }

  // Fallback: If we have a very long first segment with numbers, maybe it needs a dash
  // e.g. 90765640 -> 907656-40
  if (validTokens.length > 0 && validTokens[0].length >= 8 && /^\d+$/.test(validTokens[0])) {
    const s = validTokens[0];
    candidates.push(`${s.substring(0, 6)}-${s.substring(6)}`);
  }

  // Proactive Color Extraction for the UI column
  // Often the 2nd or 3rd token is the color code
  const colorCandidate = allTokens.find((t, idx) => idx > 0 && isLikelyColor(t));
  if (colorCandidate) {
    colorCode = normalizeColor(colorCandidate);
  } else {
    // Check if the second token is the color even if it doesn't meet strict rules
    if (validTokens.length > 1 && validTokens[1].length <= 5) {
      colorCode = validTokens[1].toUpperCase();
    }
  }

  // Deduplicate and filter empty candidates
  const uniqueCandidates = Array.from(new Set(candidates)).filter(c => c.length > 0);

  return {
    fileName: inputName,
    fullPath: path,
    brandHint,
    candidateCodes: uniqueCandidates,
    productCode: uniqueCandidates[0] || "", 
    colorCode: colorCode,
    tokens: allTokens
  };
};

export const parseCSV = (csvText: string): any[] => {
  const lines = csvText.split(/\r?\n/);
  if (lines.length < 2) return [];

  const delimiter = lines[0].includes('\t') ? '\t' : ',';
  const headers = lines[0].split(delimiter).map(h => h.trim().toLowerCase());
  
  return lines.slice(1).filter(line => line.trim()).map(line => {
    const values = line.split(delimiter);
    const obj: any = {};
    headers.forEach((header, i) => {
      obj[header] = values[i] ? values[i].trim() : "";
    });
    return obj;
  });
};
