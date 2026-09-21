import assert from "node:assert/strict";
import { buildCurl, classifyUrl } from "../browser_extension/capture.js";
import { shouldAutoAttach, supportsPageUrl } from "../browser_extension/auto_listen.js";
import { createSingleFlight } from "../browser_extension/single_flight.js";

assert.equal(supportsPageUrl("https://bscm.jinritemai.com/views/cargo-right-transfer/list"), true);
assert.equal(supportsPageUrl("https://web.jackyun.com/home/mainframe_web.html"), true);
assert.equal(supportsPageUrl("https://example.com/"), false);
assert.equal(supportsPageUrl("chrome://extensions/"), false);
assert.equal(shouldAutoAttach("https://bscm.jinritemai.com/views/", false), true);
assert.equal(shouldAutoAttach("https://bscm.jinritemai.com/views/", true), false);

let attachCalls = 0;
const attachOnce = createSingleFlight(async tabId => {
  attachCalls += 1;
  await new Promise(resolve => setTimeout(resolve, 10));
  return tabId;
});
const concurrentResults = await Promise.all([attachOnce(7788), attachOnce(7788), attachOnce(7788)]);
assert.deepEqual(concurrentResults, [7788, 7788, 7788]);
assert.equal(attachCalls, 1);

assert.equal(
  classifyUrl("https://bscm.jinritemai.com/api/gei/generalExport?subject_aid=305219").name,
  "BSCM货权采购导出"
);
assert.equal(classifyUrl("https://example.com/api/gei/generalExport"), null);

const curl = buildCurl(
  {
    url: "https://bscm.jinritemai.com/api/gei/generalExport?subject_aid=305219",
    method: "GET",
    headers: { "User-Agent": "test-agent", Referer: "https://bscm.jinritemai.com/" }
  },
  { Cookie: "sessionid=test-cookie" }
);
assert.match(curl, /generalExport/);
assert.match(curl, /-b "sessionid=test-cookie"/);
assert.doesNotMatch(curl, /example\.com/);

console.log("browser extension capture tests: OK");
