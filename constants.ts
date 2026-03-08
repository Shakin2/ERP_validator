
export const COLOR_MAPPINGS: Record<string, string> = {
  'BK': 'BLK',
  'WNV': 'NVY',
  'WH': 'WHT',
  'BL': 'BLU',
  'RD': 'RED',
  'GR': 'GRN',
  'WT': 'WHT',
  'OR': 'ORG',
  'GY': 'GRY',
  'PK': 'PNK',
  'VOL': 'VOLT',
  'FSA': 'FUSCHIA',
  'MULTI': 'MULTI'
};

export const BRANDS_WITH_DASH_RULE = ['SKECHERS', 'HOKA', 'SAUCONY', '2XU'];

// Filename prefixes that should be stripped before parsing (case-insensitive)
export const STRIP_PREFIXES = ['MRLW'];

export const NOISE_WORDS = new Set([
  'NIKE', 'ADIDAS', 'SKECHERS', 'ASICS', 'HOKA', 'SAUCONY', '2XU',
  'AURORA', 'AIR', 'MAX', 'VAPOR', 'FLYKNIT', 'PLUS', 'IMAGERY',
  'SAMPLE', 'FINAL', 'EDIT', 'WEB', 'HIGH', 'LOW', 'RETRO', 'RUN',
  'TRAINING', 'FOOTWEAR', 'APPAREL', 'CORE', 'COMP'
]);

export const DELIMITERS = /[_.\-\s]+/;
