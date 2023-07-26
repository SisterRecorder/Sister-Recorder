# Sister-Recorder

Sister Recorder是一个只录制B站HLS直播流的录播软件，可以用作某录播姬暂不支持HLS的当下的备选方案之一

## 依赖

Sister Recorder通过调用外部的streamlink实现录制，因而需要下载或安装相应的依赖软件
- [streamlink](https://streamlink.github.io/install.html)

## 如何使用

安装依赖软件后，在urls.txt中填入需要监视的直播间链接，每行一条，然后运行main.py
可使用`SISREC_PROXY_URL`环境变量为API配置正向代理URL

## Wishlist

Sister Recorder目前是一个简单赶工实现的录播姬，所以很多功能都没有实现，这些是可能在下一步添加和实现的功能：
- Cookies支持
- 简单的WebUI
- ffmpeg直接录制/录制后自动转封装
- 反代API支持
- 导出对应场次弹幕
