
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
 * Structural fuzzy matching for product codes.
 *
 * Splits codes on '-' and requires the base style code (first segment)
 * to match EXACTLY. Only allows fuzziness (distance ≤1) on the
 * remaining variant/colour segments.
 *
 * e.g. "ABC-123" will only match candidates starting with "ABC-",
 * and only if the suffix ("123") is within 1 edit of the candidate's suffix.
 *
 * For codes without a '-', falls back to exact-prefix matching:
 * the candidate must start with the target (or vice versa) and differ
 * by at most 1 trailing character.
 */
export const findBestFuzzyMatch = (
  target: string,
  candidates: string[],
  threshold: number = 2
): { code: string; distance: number } | null => {
  const upperTarget = target.toUpperCase();
  const targetParts = upperTarget.split('-');
  const targetBase = targetParts[0];
  const targetSuffix = targetParts.slice(1).join('-');

  let bestMatch: string | null = null;
  let minDistance = Infinity;

  for (const candidate of candidates) {
    const upperCandidate = candidate.toUpperCase();
    if (upperCandidate === upperTarget) continue; // skip exact matches (handled elsewhere)

    const candidateParts = upperCandidate.split('-');
    const candidateBase = candidateParts[0];
    const candidateSuffix = candidateParts.slice(1).join('-');

    // Base style code must match exactly
    if (targetBase !== candidateBase) continue;

    // Both have suffixes — compare them with tight threshold (≤1 edit)
    if (targetSuffix && candidateSuffix) {
      const suffixDistance = getLevenshteinDistance(targetSuffix, candidateSuffix);
      if (suffixDistance <= 1 && suffixDistance < minDistance) {
        minDistance = suffixDistance;
        bestMatch = candidate;
      }
      continue;
    }

    // Target has suffix but candidate doesn't (or vice versa) —
    // only match if the suffix is very short (1 char), treating it as distance 1
    if (targetSuffix && !candidateSuffix && targetSuffix.length <= 1) {
      if (1 < minDistance) {
        minDistance = 1;
        bestMatch = candidate;
      }
    } else if (!targetSuffix && candidateSuffix && candidateSuffix.length <= 1) {
      if (1 < minDistance) {
        minDistance = 1;
        bestMatch = candidate;
      }
    }
  }

  if (bestMatch && minDistance <= 1) {
    return { code: bestMatch, distance: minDistance };
  }

  return null;
};
