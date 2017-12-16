# Downloader
简单使用的文件下载器

## 运行环境
+ Python 3.5.X及其以上版本
+ 第三方库安装：pip3 install aiohttp requests shutil async_timeout redis click psutil

## 基本原理
**文件下载时，大部分服务器支持文件分块下载，可通过HTTP的Range请求头设置需要下载的字节块，因此可事先请求获取下载文件字节大小，然后并发分块下载，下载完成后对数据块进行合成，提高下载速度**

## 支持下载模式
+ 单文件下载模式：将单个文件拆分为多个数据块进行下载然后合成原文件
+ 多文件下载模式：多个文件并发下载，每一个文件拆分为多个数据块下载合成
+ 多文件分布式下载：利用redis数据库作为消息队列分发任务，分布式下载多个文件
+ 单文件分布式下载：利用redis数据库作为消息队列分发任务，分布式下载数据块，然后在同一个主机上面进行合成，适用于大文件下载
+ 文件下载结束前会生成临时文件或者临时文件夹，用来保存临时数据，程序终止时文件未下载完成可继续下载，实现断点续传

## 参数说明
+ --mode 必选参数，选择下载模式，共五种模式[one, more, put, redis, one_redis, mix]
+ --url TEXT                      文件下载地址
+ --oworkers INTEGER              单个文件分块下载协程数，默认为10
+ --block_size INTEGER            分块字节大小，默认为124*100字节，100KB
+ --size INTEGER                  下载文件总字节大小
+ --tfolder TEXT                  文件下载临时目录
+ --name TEXT                     指定下载文件保存名
+ --files TEXT                    多文件下载要下载的文件列表，格式为json
+ --fworkers INTEGER              多文件下载协程数，默认为10
+ --tfile TEXT                    多文件下载临时记录文件
+ --key TEXT                      分布式下载时redis键值
+ --host TEXT                     分布式下载时redis主机地址，默认为127.0.0.1
+ --port INTEGER                  分布式下载时redis端口，默认为6379
+ --db INTEGER                    分布式下载时redis数据库，默认为0
+ --password TEXT                 分布式下载时redis密码，默认为None
+ --help                          帮助文档

## 举例说明
### 单文件下载模式
**python3 downloader.py --mode=one --url=https://www.python.org/ftp/python/3.6.4/Python-3.6.4rc1.tar.xz**
+ 结果如下：
```
[+] 正在下载文件:Python-3.6.4rc1.tar.xz，临时文件目录:temp_a8566e
[+] 下载完毕，正在合成文件:Python-3.6.4rc1.tar.xz
[+] Python-3.6.4rc1.tar.xz合成完成
[+] 总耗时:43.41S
[+] 平均速度:382.58KB/S
```
+ 下载时会显示下载进度:[+] 10/167 12.56% 0.13S/B 23S >=34S ; 分别表示:[+] 已下载数据块/总数据块 下载百分比 每一块下载所需时间 已用时间 预计下载完成需要时间
+ 下载未完成终止可通过指定临时文件目录继续下载:<br>
**python3 downloader.py --mode=one --url=https://www.python.org/ftp/python/3.6.4/Python-3.6.4rc1.tar.xz --tfolder=temp_a8566e**
<br>
+ 该下载模式下必选参数有:mode, url; 可选参数有:tfolder, oworkers, block_size, size, name

### 多文件下载模式
+ 多文件下载模式需要提供特定格式的下载文件，格式为json，每一条json数据中url字段是必须的，size表示下载文件总字节大小，name表示下载完成后文件保存名称，size和name字段可选，具体如下：
```
{"url": "https://www.python.org/ftp/python/3.6.4/Python-3.6.4rc1.tar.xz", "size":234234, "name":"Python-3.6.4.tar.xz"}
{"url": "https://www.python.org/ftp/python/3.6.4/Python-3.6.4rc1.tgz"}
{"url": "https://www.python.org/ftp/python/3.7.0/Python-3.7.0a3.tar.xz"}
{"url": "https://www.python.org/ftp/python/3.7.0/Python-3.7.0a3.tgz"}
{"url": "https://www.python.org/ftp/python/3.6.3/Python-3.6.3.tar.xz"}
```
