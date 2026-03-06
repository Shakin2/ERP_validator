
/**
 * Calculates the Levenshtein distance between two strings.
 */
export const getLevenshteinDistance = (a: string, b: string): number => {
  const matrix: number[][] = [];

  for (let i = 0; i <= b.length; i++) {
    matrix[i] = [i];
  }

  for (let j = 0; j <= a.length; j++) {
    matrix[0][j] = j;
  }

  for (let i = 1; i <= b.length; i++) {
    for (let j = 1; j <= a.length; j++) {
      if (b.charAt(i - 1) === a.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1, // substitution
          matrix[i][j - 1] + 1,     // insertion
          matrix[i - 1][j] + 1      // deletion
        );
      }
    }
  }

  return matrix[b.length][a.length];
};

/**
 * Finds the best match in a list of codes based on Levenshtein distance.
 */
export const findBestFuzzyMatch = (
  target: string,
  candidates: string[],
  threshold: number = 2
): { code: string; distance: number } | null => {
  let bestMatch = null;
  let minDistance = Infinity;

  for (const candidate of candidates) {
    const distance = getLevenshteinDistance(target.toUpperCase(), candidate.toUpperCase());
    if (distance < minDistance) {
      minDistance = distance;
      bestMatch = candidate;
    }
  }

  if (bestMatch && minDistance <= threshold) {
    return { code: bestMatch, distance: minDistance };
  }

  return null;
};
