# 安全与凭据管理

## 不得提交的内容

- 吉客云 cURL、authorization、cookie、token、签名密钥和 `commonVerify`。
- MySQL、DolphinScheduler 或其他服务的账号密码。
- `manual_inputs/` 中的业务 Excel、导出报表和运行日志。
- 本机 IDE、Agent 权限配置和服务器数据卷。

## 配置方式

复制 `.env.example` 中的变量名，在本地 Shell、容器编排或 DolphinScheduler 环境中设置真实值。项目不会把真实 `.env` 纳入 Git。

浏览器登录态保存在运行节点的 `curl/` 目录；该目录整体被 `.gitignore` 排除。脚本内的 `START_*_CURL_TEXT` 只能为空字符串，不得作为长期登录态存储。

## 泄漏处置

一旦凭据进入工作区、提交历史、日志或远端仓库：

1. 立即撤销或轮换对应数据库密码、API 密钥和网页登录态。
2. 清理当前提交以及必要的 Git 历史。
3. 完成安全扫描后再推送。

仅删除当前文件中的秘密并不能让已经进入 Git 历史的秘密失效。
