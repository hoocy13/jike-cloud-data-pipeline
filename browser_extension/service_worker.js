import { buildCurl, classifyUrl } from "./capture.js";
import { shouldAutoAttach } from "./auto_listen.js";
import { createSingleFlight } from "./single_flight.js";

const API = "http://127.0.0.1:18765/capture";
const pending = new Map();
const attachedTabs = new Set();

async function isPaused(tabId) {
  const { pausedTabIds = [] } = await chrome.storage.session.get("pausedTabIds");
  return pausedTabIds.includes(tabId);
}

async function setPaused(tabId, paused) {
  const { pausedTabIds = [] } = await chrome.storage.session.get("pausedTabIds");
  const next = new Set(pausedTabIds);
  if (paused) next.add(tabId); else next.delete(tabId);
  await chrome.storage.session.set({ pausedTabIds: [...next] });
}

async function setStatus(tabId, text, color) {
  await chrome.action.setBadgeText({ tabId, text });
  await chrome.action.setBadgeBackgroundColor({ tabId, color });
  const status = { lastStatus: text, lastUpdated: new Date().toLocaleString() };
  if (text !== "!") status.lastError = null;
  await chrome.storage.local.set(status);
}

async function attach(tabId) {
  if (await isAttached(tabId)) return;
  await chrome.debugger.attach({ tabId }, "1.3");
  await chrome.debugger.sendCommand({ tabId }, "Network.enable");
  attachedTabs.add(tabId);
  await setStatus(tabId, "ON", "#1677ff");
}

const attachOnce = createSingleFlight(attach);

async function isAttached(tabId) {
  if (attachedTabs.has(tabId)) return true;
  const targets = await chrome.debugger.getTargets();
  const attached = targets.some(target => target.tabId === tabId && target.attached);
  if (attached) attachedTabs.add(tabId);
  return attached;
}

async function autoAttach(tab) {
  if (!tab?.id || !shouldAutoAttach(tab.url || "", await isPaused(tab.id))) return;
  try {
    await attachOnce(tab.id);
  } catch (error) {
    await chrome.storage.local.set({ lastError: error.message, lastUpdated: new Date().toLocaleString() });
    await setStatus(tab.id, "!", "#dc2626");
  }
}

async function attachExistingTabs() {
  const tabs = await chrome.tabs.query({});
  await Promise.all(tabs.map(autoAttach));
}

async function detach(tabId) {
  if (!attachedTabs.has(tabId)) return;
  await chrome.debugger.detach({ tabId });
  attachedTabs.delete(tabId);
  await setStatus(tabId, "", "#999999");
}

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  (async () => {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!tab?.id) throw new Error("没有活动标签页");
    if (message.action === "toggle") {
      if (await isAttached(tab.id)) {
        await setPaused(tab.id, true);
        await detach(tab.id);
      } else {
        await setPaused(tab.id, false);
        await attachOnce(tab.id);
      }
    }
    sendResponse({ ok: true, listening: await isAttached(tab.id) });
  })().catch(error => sendResponse({ ok: false, error: error.message }));
  return true;
});

chrome.debugger.onDetach.addListener(({ tabId }) => {
  attachedTabs.delete(tabId);
  chrome.action.setBadgeText({ tabId, text: "" });
});

chrome.tabs.onCreated.addListener(autoAttach);
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.url || changeInfo.status === "complete") autoAttach({ ...tab, id: tabId });
});
chrome.tabs.onRemoved.addListener(tabId => {
  attachedTabs.delete(tabId);
  setPaused(tabId, false);
});
chrome.runtime.onInstalled.addListener(attachExistingTabs);
chrome.runtime.onStartup.addListener(attachExistingTabs);
attachExistingTabs();

async function sendCapture(tabId, requestId) {
  const item = pending.get(requestId);
  if (!item?.request || item.sent) return;
  item.sent = true;
  try {
    const curl = buildCurl(item.request, item.extraHeaders || {});
    const response = await fetch(API, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ curl, name: item.target.name })
    });
    if (!response.ok) throw new Error(await response.text());
    await setStatus(tabId, "OK", "#16a34a");
  } catch (error) {
    item.sent = false;
    await chrome.storage.local.set({ lastError: error.message, lastUpdated: new Date().toLocaleString() });
    await setStatus(tabId, "!", "#dc2626");
  } finally {
    setTimeout(() => pending.delete(requestId), 10000);
  }
}

chrome.debugger.onEvent.addListener(async (source, method, params) => {
  if (!source.tabId) return;
  if (method === "Network.requestWillBeSent") {
    const target = classifyUrl(params.request.url);
    if (!target) return;
    const previous = pending.get(params.requestId) || {};
    pending.set(params.requestId, { ...previous, request: params.request, target, sent: false });
    setTimeout(() => sendCapture(source.tabId, params.requestId), 400);
  } else if (method === "Network.requestWillBeSentExtraInfo") {
    const item = pending.get(params.requestId) || { sent: false };
    item.extraHeaders = params.headers || {};
    pending.set(params.requestId, item);
    await sendCapture(source.tabId, params.requestId);
  }
});
