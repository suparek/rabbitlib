基于pika的rabbitmq消息队列的类库
支持 阿里云rabbitmq实例 与 自建的rabbitmq服务


### 注意
如果要在自建的rabbitmq使用 延时队列 功能，需要rabbitmq server 安装 rabbitmq-delayed-message-exchange 插件
https://blog.csdn.net/wangming520liwei/article/details/103352440

阿里云自带延时队列功能，可直接支持

使用时，可参考 test 文件夹下的示例

### rabbitmq使用常犯错误参考
https://www.cloudamqp.com/blog/part4-rabbitmq-13-common-errors.html