const SUPPORTED_ORIGINS = new Set([
  "https://web.jackyun.com",
  "https://bscm.jinritemai.com"
]);

export function supportsPageUrl(rawUrl) {
  try {
    return SUPPORTED_ORIGINS.has(new URL(rawUrl).origin);
  } catch {
    return false;
  }
}

export function shouldAutoAttach(rawUrl, paused) {
  return !paused && supportsPageUrl(rawUrl);
}
