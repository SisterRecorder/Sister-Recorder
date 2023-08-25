# Sister-Recorder

Sister Recorder是一个只录制B站HLS直播流的录播软件

Sister Recorder is an alternative bilibili livestream recorder that aim to record and only record HLS streams.

这个项目主要是作为async + 多playlist可靠性的验证方案

This project is intended as a proof-of-concept solution to evaluate reliability of async-based multi-playlist solution.


## 依赖

Sister Recorder实现了一个HLS下载器，但也可通过调用外部的streamlink/ffmpeg实现录制，如需使用需要下载或安装相应的依赖软件
- [streamlink](https://streamlink.github.io/install.html)
- [ffmpeg](https://ffmpeg.org/download.html)

Release中的压缩包已经打包了必须的依赖。如果使用Release中打包的版本，则无需额外下载依赖。

## 如何使用

安装依赖软件后，在urls.txt中填入需要监视的直播间链接，每行一条，然后运行main.py。或直接从[Release](https://github.com/SisterRecorder/Sister-Recorder/releases)下载打包后的软件解压运行。

相关的配置在config.example.ini中可以看到。新建config.ini并填入配置即可在启动后生效。

<!-- ## Wishlist

Sister Recorder目前是一个简单赶工实现的项目，所以很多功能都没有实现，这些是可能在下一步添加和实现的功能：
- 简单的WebUI
- 录制后自动转封装
- 导出对应场次弹幕 -->
