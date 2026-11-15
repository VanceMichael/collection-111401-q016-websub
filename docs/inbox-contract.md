# 收件订阅约定

收件服务把每个订阅登记为本地记录，回调地址形如 `/callback/{id}`；管理与接收入口只绑定本机地址。订阅状态机：`pending → active →（stopping）→ stopped`，验证失败为 `failed`。

## 对账为准

- 来源的验证 GET 只证明回调可达：响应原样返回 `hub.challenge`，但**验证请求不改变本地订阅状态**。生效、续租、停止都以来源的订阅记录对账（`POST /subscriptions/{id}/sync`，或接入、续租、停止时自动对账）为准。
- 租期自对账确认起按验证参数 `hub.lease_seconds` 以本地时钟计。过期订阅的投递被拒绝；`POST /subscriptions/{id}/renew` 重新登记，确认后租约顺延，期间接收不中断。
- `POST /subscriptions/{id}/rotate` 以新密钥建立接替订阅。旧订阅在停止或过期前仍会投递一阵：同一资料（publisher + id）只收存一次，不同发布者的同名记录各自保留。
- `POST /subscriptions/{id}/stop` 登记退订后立即拒收；对账确认退订记录后落定 `stopped`。迟到投递记入拒绝日志，重启后停止状态不恢复。

## 接收判定

`POST /callback/{id}` 仅在订阅 `active` 且租约未过期、`X-Hub-Signature-256` 与登记密钥匹配、资料身份（publisher、id）完整时收存。内容落盘（写入并 fsync）成功才返回 2xx；重复资料返回 200 并标记 `duplicate`，不重复收存。过期、尚未确认、已停止、签名不符的投递一律拒绝并留痕。

## 查看

`GET /subscriptions` 查看订阅状态与收存、去重、拒绝计数；`GET /materials` 查看实际收存的资料凭据；`GET /rejections`（可按 `?subscription=` 过滤）查看被拒绝的迟到与异常请求。所有接口均不返回订阅密钥。
