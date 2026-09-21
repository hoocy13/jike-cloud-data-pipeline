const status = document.querySelector("#status");
document.querySelector("#toggle").addEventListener("click", async () => {
  const result = await chrome.runtime.sendMessage({ action: "toggle" });
  status.textContent = result.ok ? (result.listening ? "当前标签页：监听中" : "当前标签页：已停止") : `错误：${result.error}`;
});
chrome.storage.local.get(["lastStatus", "lastUpdated", "lastError"]).then(value => {
  status.textContent = value.lastError ? `最近错误：${value.lastError}` : `最近状态：${value.lastStatus || "未捕获"} ${value.lastUpdated || ""}`;
});
