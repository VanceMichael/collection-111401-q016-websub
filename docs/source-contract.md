# 公开目录订阅

`/publisher` 返回发布者、topic 与 hub。订阅按 WebSub 的表单参数提交到 `/hub`：hub.mode、hub.topic、hub.callback、hub.secret。202 只是收到了请求。随后 hub 向 callback GET 发送验证参数，响应必须原样返回 hub.challenge；真正的订阅状态有独立记录，租期从验证成功起按来源时钟计。

本地演练通过 POST `/verify/{id}` 触发验证、POST `/deliver/{id}` 触发投递，均属于来源操作者动作，不是收件端的交付回执。投递体是 JSON，X-Hub-Signature-256 为原始请求字节的 HMAC-SHA256。资料身份属于 publisher，不能跨发布者仅按 id 去重。`/subscriptions` 不返回 secret。

推送成功只认收件端 2xx。旧订阅在续租和停止期间仍可能发出迟到消息；接收端需按自己的已确认租约与停止状态判断，不依赖 hub 永不迟到。所有回调仅允许本地地址。

## 本地演练控制

`GET /_health` 返回服务健康状态；`GET /_audit` 仅返回请求路径、方法和体积，不记录凭据。`POST /_control` 接收 JSON，可调整 generation、mode、clock。控制端点只服务于本地演练，不是资料接口，也不是浏览器业务操作入口。每次重新启动服务都会恢复初始数据；生产接入时不部署此控制面。
