// types.ts

export enum FileStatus {
  IDLE = 'IDLE',
  PARSING = 'PARSING',
  QUERYING_KEYS = 'QUERYING_KEYS',
  QUERYING_DETAILS = 'QUERYING_DETAILS',
  PROCESSING = 'PROCESSING',
  COMPLETE = 'COMPLETE',
  ERROR = 'ERROR'
}

export type MatchStatus = 'SUCCESS' | 'FUZZY' | 'Multi Colour AP21 - Cant Find' | 'Multi Colour AP21 - No Reference' | 'Multi Colour in Name' | 'FAILURE';

export interface ERPRecord {
  productCode: string;
  styleCode: string;
  clrCode: string;
  
  // All required fields
  name: string;
  brand: string;
  category: string;
  clridx: string;
  colourCode: string;
  gender: string;
  productEndUse: string;
  productModel: string;
  productRange: string;
  productSubCategory: string;
  productType: string;
  styleColour: string;
  styleSubRange: string;
  
  // Legacy compatibility
  styleCategory: string;
  productName: string;
}

export interface ExtractedInfo {
  fileName: string;
  filePath: string;
  brandHint: string;
  colorCode: string;
  candidateCodes: string[];
}

export interface MatchResult extends ExtractedInfo {
  attempts: string[];
  isMatch: boolean;
  isFuzzy: boolean;
  fuzzyMatchCode?: string;
  
  // All ERP fields from ERPRecord
  productCode?: string;
  name?: string;
  brand?: string;
  category?: string;
  clridx?: string;
  colourCode?: string;
  gender?: string;
  productEndUse?: string;
  productModel?: string;
  productRange?: string;
  productSubCategory?: string;
  productType?: string;
  styleColour?: string;
  styleSubRange?: string;
  
  // Legacy fields (for backwards compatibility)
  erpBrand?: string;
  erpStyleCategory?: string;
  erpProductName?: string;
  
  colorVariantCount?: number;
  status: MatchStatus;
  reason: string;
}

export interface QueryLog {
  batchId: number;
  stage: 'KEYS' | 'DETAILS';
  timestamp: string;
  statement: string;
  rawResponse: any;
  parsedCount: number;
  error?: string;
}