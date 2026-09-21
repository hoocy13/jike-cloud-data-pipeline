export const TARGETS = [
  ["https://web.jackyun.com", "startExcelExport", "吉客云销售导出"],
  ["https://bscm.jinritemai.com", "exportFulfillOrderList", "BSCM正向全链路"],
  ["https://bscm.jinritemai.com", "/api/procurement/po/list", "BSCM货权采购查询"],
  ["https://bscm.jinritemai.com", "/api/gei/generalExport", "BSCM货权采购导出"]
];

export function classifyUrl(rawUrl) {
  const url = new URL(rawUrl);
  const match = TARGETS.find(([origin, endpoint]) => url.origin === origin && url.pathname.includes(endpoint));
  return match ? { endpoint: match[1], name: match[2] } : null;
}

function quote(value) {
  return JSON.stringify(String(value));
}

export function buildCurl(request, extraHeaders = {}) {
  const merged = new Map();
  for (const [name, value] of Object.entries(request.headers || {})) {
    merged.set(name.toLowerCase(), [name, value]);
  }
  for (const [name, value] of Object.entries(extraHeaders || {})) {
    merged.set(name.toLowerCase(), [name, value]);
  }
  const cookie = merged.get("cookie")?.[1] || "";
  merged.delete("cookie");
  const allowed = new Set([
    "accept", "accept-language", "authorization", "ati", "bx-v", "content-type",
    "menukey", "module_code", "origin", "referer", "user-agent", "x-requested-with"
  ]);
  const parts = ["curl", "--url", quote(request.url), "-X", quote(request.method || "GET")];
  for (const [lower, [name, value]] of merged.entries()) {
    if (allowed.has(lower)) parts.push("-H", quote(`${name}: ${value}`));
  }
  if (cookie) parts.push("-b", quote(cookie));
  if (request.postData) parts.push("--data-raw", quote(request.postData));
  return parts.join(" ") + "\n";
}
