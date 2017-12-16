# coding=UTF-8

import aiohttp
import asyncio
import uuid
import requests
import shutil
from asyncio import Queue
import async_timeout
import time
import sys
import os
import json
import hashlib
import redis
import click
import psutil
import traceback

"""
支持模式:
one:单文件单机分块下载，适用于单个小文件下载，通过指定临时目录进行断点续传
more:多文件单机分块下载，适用于多文件下载，通过指定临时文件进行断点续传
put:将待下载文件列表上传至redis进行分布式下载，适用于多文件下载
redis:多文件分布式下载，适用于多文件下载
one_redis:单个文件分布式分块下载然后在同一个主机上面进行合成文件，适用于单个大文件下载
mix:单个文件合成
"""

# 所有文件的数据分块
ALL_BLOCKS = 0
# 已经下载完的分块
HAS_BLOCKS = 1
# 开始时间
START = time.time()
# 所有文件的大小列表
ALL_SIZES = list()


def get_macid():
    """将计算机网卡MAC地址的hash作为唯一的识别码，网卡不能在下载期间变动"""
    ifaces = psutil.net_if_addrs()
    macs = list()
    for iface, info in ifaces.items():
        if iface == 'lo':
            continue
        ipv4_mac = info[-1].address
        macs.append(ipv4_mac)
    macs.sort()
    mac_str = ':'.join(macs)
    hash_str = hashlib.md5(mac_str.encode('UTF-8')).hexdigest()
    return hash_str


def make_block(size, block_size=1024 * 100, temp_folder=None, is_continue=False):
    """将数据分块"""
    global ALL_BLOCKS
    block_names = list()
    if is_continue:
        blocks = os.scandir('./' + temp_folder)
        block_names = [int(b.name.split('.')[0]) for b in blocks]
    rg = 0
    for i, r in enumerate(range(rg, size + 1, block_size)):
        if i in block_names:
            continue
        if r == 0 and size <= block_size:
            hr = '0-'
        elif r == 0 and size > block_size:
            hr = '0-' + str(block_size)
        elif r + block_size >= size:
            hr = str(r + 1) + '-'
        else:
            hr = str(r + 1) + '-' + str(r + block_size)
        ALL_BLOCKS += 1
        yield (str(i) + ' ' + hr)


def mixfiles(name, temp_folder):
    """将小文件快合成大文件"""
    if os.path.exists(name):
        name = name + '_' + str(uuid.uuid4()).replace('-', '')[0:6]
    if not os.path.exists(temp_folder):
        return
    print('[+] 下载完毕，正在合成文件:{name}'.format(name=name))
    blocks = os.scandir('./' + temp_folder)
    block_names = [b.name for b in blocks]
    block_names.sort(key=lambda x: int(x.split('.')[0]))
    with open(name, 'ab+') as m:
        for block in block_names:
            with open(temp_folder + '/' + block, 'rb') as b:
                data = b.read()
                m.write(data)
    shutil.rmtree(temp_folder)
    print('[+] {name}合成完成'.format(name=name))


def read_files(filenames):
    """读取需要下载的json文件列表"""
    if not os.path.exists(filenames):
        print('[ERROR] {filenames}文件不存在'.format(filenames=filenames))
        exit(0)
    with open(filenames, 'r', encoding="UTF-8") as f:
        for i in f:
            if i and i.strip():
                try:
                    file = json.loads(i.strip())
                    yield file
                except Exception as e:
                    print('[ERROR] read_files error:' + str(e))
                    exit(0)


def get_size(url):
    """获取文件size大小"""
    try:
        req = requests.get(url, headers={'Range': 'bytes=0-10'}, timeout=10)
        content_range = req.headers['Content-Range']
        size = int(content_range.split('/')[-1])
        ALL_SIZES.append(size)
        return size
    except Exception as e:
        print(e)
        return None


async def get_block(url, ranges):
    """传入连接和要下载的地址，然后进行分块下载"""
    global HAS_BLOCKS
    try:
        headers = {'Range': 'bytes={hr}'.format(hr=ranges)}
        with async_timeout.timeout(60):
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    datas = await resp.read()
                    HAS_BLOCKS += 1
                    return datas
    except Exception as e:
        datas = await get_block(url, ranges)
        return datas


async def worker_one(block_queues, url, temp_folder):
    """下载数据块，供单个文件下载使用"""
    while True:
        # 显示下载进度
        now = time.time()
        all_time = ALL_BLOCKS / HAS_BLOCKS * (now - START)
        need_time = all_time - (now - START)
        process = '[+] {0} {1:.2f}% {2:.2f}S/B {3}S >={4}S'.format(
            str(HAS_BLOCKS - 1) + '/' + str(ALL_BLOCKS), (HAS_BLOCKS / ALL_BLOCKS * 100),
            (now - START) / HAS_BLOCKS, int(now - START), int(need_time))
        sys.stdout.write(process + '\b' * len(process))
        sys.stdout.flush()

        i_hr = await block_queues.get()
        i, hr = i_hr.split()
        block = await get_block(url, hr)
        with open(temp_folder + '/' + str(i) + '.dat', 'ab+') as f:
            f.write(block)
        block_queues.task_done()


async def run_one(url, size, one_workers, block_size, temp_folder, is_continue):
    """分配创建任务，将一个文件分块下载"""
    block_queues = Queue()
    await asyncio.wait([block_queues.put(i) for i in make_block(size, block_size, temp_folder, is_continue)])
    tasks = [asyncio.ensure_future(worker_one(block_queues, url, temp_folder)) for i in range(one_workers)]
    await block_queues.join()
    for task in tasks:
        task.cancel()


async def download_one(url, block_size, one_workers, size, temp_folder, name):
    """下载单个文件"""
    if not temp_folder:
        temp_folder = 'temp_' + str(uuid.uuid4()).replace('-', '')[0:6]
        os.mkdir(temp_folder)
        is_continue = False
    else:
        is_continue = True

    if not size:
        size = get_size(url)
        if not size:
            print('[+] 无法获取下载文件大小，错误文件error_urls.err')
            error_urls = {'url': url}
            if size:
                error_urls['size'] = size
            if temp_folder:
                error_urls['temp_folder'] = temp_folder
            if name:
                error_urls['name'] = name
            with open('error_urls.err', 'a', encoding='UTF-8') as f:
                f.write(json.dumps(error_urls) + '\n')
            return
    if not name:
        name = url.split('/')[-1]

    print('[+] 正在下载文件:{name}，临时文件目录:{temp_folder}'.format(name=name, temp_folder=temp_folder))
    task = [asyncio.ensure_future(run_one(url, size, one_workers, block_size, temp_folder, is_continue))]
    await asyncio.wait(task)

    mixfiles(name, temp_folder)


async def worker_more(files_queues, block_size, one_workers):
    """下载数据块，共多个文件下载使用"""
    while True:
        file = await files_queues.get()
        url = file['url']
        size = file.get('size', None)
        name = file.get('name', None)
        temp_folder = file['temp_folder']
        await download_one(url, block_size, one_workers, size, temp_folder, name)
        files_queues.task_done()


async def run_more(file_list, file_workers, block_size, one_workers):
    """同事下载下载多个文件"""
    files_queues = Queue()
    await asyncio.wait([files_queues.put(i) for i in file_list])
    tasks = [asyncio.ensure_future(worker_more(files_queues, block_size, one_workers)) for i in range(file_workers)]
    await files_queues.join()
    for task in tasks:
        task.cancel()


async def download_more(files, file_workers, temp_file, block_size, one_workers):
    """同时下载多个文件
    files:需要下载的文件列表
    temp_file:用来记录每一个文件的下载临时地址,如果存在改文件则读取，不存在则生成
    file_workers:下载文件的线程数
    """
    file_list = list()
    if not temp_file:
        temp_file = 'temp_' + str(uuid.uuid4()).replace('-', '')[0:6] + '.txt'
        with open(temp_file, 'a', encoding='UTF-8') as t:
            for name in read_files(files):
                temp_folder = 'temp_' + str(uuid.uuid4()).replace('-', '')[0:6]
                os.mkdir(temp_folder)
                name['temp_folder'] = temp_folder
                file_list.append(name)
                t.write(json.dumps(name) + '\n')
        print('[+] 生成临时文件:' + temp_file)
    else:
        # 校验临时文件中的临时目录是否存在
        all_file = os.listdir('.')
        temp_folders = list()
        for f in all_file:
            if os.path.isdir(f) and f.startswith('temp_'):
                temp_folders.append(f)
        for name in read_files(temp_file):
            if name['temp_folder'] in temp_folders:
                file_list.append(name)

    task = [asyncio.ensure_future(run_more(file_list, file_workers, block_size, one_workers))]
    await asyncio.wait(task)

    os.remove(temp_file)


def download_put(files, key, host, port, db, password):
    """将文件内容上传到redis中"""
    pool = redis.ConnectionPool(host=host, port=port, db=db, password=password, encoding='utf-8',
                                decode_responses=True)
    client = redis.StrictRedis(connection_pool=pool)
    for name in read_files(files):
        client.sadd(key, json.dumps(name))
    print('[+] 文件上传成功!可进行分布式运行!')


async def worker_redis(client, key, lock, temp_file, block_size, one_workers):
    """分布式多文件下载"""
    while True:
        file = client.spop(key)
        if not file:
            return
        file = json.loads(file)
        # 生成临时目录，并保存在主机id中
        temp_folder = file.get('temp_folder', None)
        if not temp_folder:
            temp_folder = 'temp_' + str(uuid.uuid4()).replace('-', '')[0:6]
            os.mkdir(temp_folder)
            file['temp_folder'] = temp_folder
            with await lock:
                with open(temp_file, 'a', encoding='UTF-8') as f:
                    f.write(json.dumps(file) + '\n')
        url = file['url']
        size = file.get('size', None)
        name = file.get('name', None)
        await download_one(url, block_size, one_workers, size, temp_folder, name)


async def download_redis(key, host, port, db, password, block_size, one_workers, file_workers):
    """将需要下载的文件上传到redis然后进行分布式下载"""
    pool = redis.ConnectionPool(host=host, port=port, db=db, password=password, encoding='utf-8',
                                decode_responses=True)
    client = redis.StrictRedis(connection_pool=pool)
    # 文件锁，生成临时文件用来记录每个任务运行结果
    temp_file = 'temp_' + str(uuid.uuid4()).replace('-', '')[0:6] + '.txt'
    print('[+] 生成临时文件:' + temp_file)
    lock = asyncio.Lock()
    tasks = [asyncio.ensure_future(worker_redis(client, key, lock, temp_file, block_size, one_workers)) for i in
             range(file_workers)]
    await asyncio.wait(tasks)

    os.remove(temp_file)


async def worker_oneredis(url, client, key, temp_folder):
    """下载数据块，供单个文件下载使用"""
    while True:
        # 显示下载进度
        now = time.time()
        finished_blocks = client.scard(key + '_finished') + 1
        all_time = ALL_BLOCKS / finished_blocks * (now - START)
        need_time = all_time - (now - START)
        process = '[+] {0} {1:.2f}% {2:.2f}S/B {3}S >={4}S'.format(
            str(finished_blocks - 1) + '/' + str(ALL_BLOCKS), (finished_blocks / ALL_BLOCKS * 100),
            (now - START) / finished_blocks, int(now - START), int(need_time))
        sys.stdout.write(process + '\b' * len(process))
        sys.stdout.flush()

        i_hr = client.spop(key)
        if not i_hr:
            return
        i, hr = i_hr.split()
        block = await get_block(url, hr)
        with open(temp_folder + '/' + str(i) + '.dat', 'ab+') as f:
            f.write(block)
        client.sadd(key + '_finished', hr)


async def download_oneredis(url, size, key, host, port, db, password, block_size, one_workers=10, temp_folder=None):
    """将单一的大文件分割成小文件分块下载，然后移动到同一个文件夹进行合成"""
    pool = redis.ConnectionPool(host=host, port=port, db=db, password=password, encoding='utf-8',
                                decode_responses=True)
    client = redis.StrictRedis(connection_pool=pool)

    if not size:
        size = get_size(url)
        if not size:
            print('[+] 无法获取下载文件大小，错误文件error_urls.err')
            error_urls = {'url': url}
            if size:
                error_urls['size'] = size
            with open('error_urls.err', 'a', encoding='UTF-8') as f:
                f.write(json.dumps(error_urls) + '\n')
            return
    # 已经下载完的分块存储在key+'_finished'中
    for rg in make_block(size, block_size):
        if not client.sismember(key + '_finished', rg):
            client.sadd(key, rg)

    if not temp_folder:
        temp_folder = 'temp_' + str(uuid.uuid4()).replace('-', '')[0:6]
        os.mkdir(temp_folder)

    print('[+] 生成临时文件目录:' + temp_folder)

    tasks = [asyncio.ensure_future(worker_oneredis(url, client, key, temp_folder)) for i in range(one_workers)]
    await asyncio.wait(tasks)

    client.delete(key + '_finished')
    print('[+] 该节点分配数据块下载完成，请准备文件合并')


@click.command()
@click.option('--mode', type=click.Choice(['one', 'more', 'put', 'redis', 'one_redis', 'mix']),
              help='必选参数，选择下载模式，共五种模式[one, more, put, redis, one_redis, mix]')
@click.option('--url', default=None, help='文件下载地址')
@click.option('--oworkers', default=10, help='单个文件分块下载协程数，默认为10')
@click.option('--block_size', default=1024 * 100, help='分块字节大小，默认为124*100字节')
@click.option('--size', type=int, default=None, help='下载文件总字节大小')
@click.option('--tfolder', default=None, help='文件下载临时目录')
@click.option('--name', default=None, help='指定下载文件保存名')
@click.option('--files', default=None, help='多文件下载要下载的文件列表，格式为json')
@click.option('--fworkers', default=10, help='多文件下载协程数，默认为10')
@click.option('--tfile', default=None, help='多文件下载临时记录文件')
@click.option('--key', default=None, help='分布式下载时redis键值')
@click.option('--host', default='127.0.0.1', help='分布式下载时redis主机地址，默认为127.0.0.1')
@click.option('--port', default=6379, type=int, help='分布式下载时redis端口，默认为6379')
@click.option('--db', default=0, type=int, help='分布式下载时redis数据库，默认为0')
@click.option('--password', default=None, help='分布式下载时redis密码，默认为None')
def main(mode, url, block_size, oworkers, size, tfolder, name, files, fworkers, tfile, key, host, port, db, password):
    try:
        if (not mode) or (mode not in ['one', 'more', 'put', 'redis', 'one_redis', 'mix']):
            print('[ERROR] mode参数为必选参数')
            exit(0)
        if tfolder and (not os.path.exists(tfolder)):
            print('[ERROR] {}目录不存在!'.format(tfolder))
            exit(0)
        if tfile and (not os.path.exists(tfile)):
            print('[ERROR] {}文件不存在!'.format(tfile))
            exit(0)

        loop = asyncio.get_event_loop()
        if mode == 'one':
            if not url:
                print('[ERROR] 请输入要下载文件的URL地址!')
                return
            loop.run_until_complete(download_one(url, block_size, oworkers, size, tfolder, name))
        elif mode == 'more':
            loop.run_until_complete(download_more(files, fworkers, tfile, block_size, oworkers))
        elif mode == 'put':
            if not key:
                print('[ERROR] PUT模式下必须指定key！')
                exit(0)
            download_put(files, key, host, port, db, password)
        elif mode == 'redis':
            if not key:
                print('[ERROR] Redis分布式下载模式下必须指定key！')
                exit(0)
            loop.run_until_complete(download_redis(key, host, port, db, password, block_size, oworkers, fworkers))
        elif mode == 'one_redis':
            if not key:
                print('[ERROR] Redis分布式下载模式下必须指定key！')
                exit(0)
            loop.run_until_complete(
                download_oneredis(url, size, key, host, port, db, password, block_size, oworkers, tfolder))
        elif mode == 'mix':
            if not name:
                print('[ERROR] mix模式下必须指定文件名name参数')
            mixfiles(name, tfolder)
        else:
            pass

        loop.close()

        needd_time = time.time() - START
        print('[+] 总耗时:{:.2f}S'.format(needd_time))
        print('[+] 平均速度:{:.2f}KB/S'.format(sum(ALL_SIZES) / 1024 / needd_time))
    except KeyboardInterrupt:
        """如果通过临时文件继续运行多文件下载，手动停止后检查哪些文件已经下载完毕，重新生成新的临时文件"""
        if tfile:
            # 当前目录下还有哪些临时目录没有运行完
            all_file = os.listdir('.')
            temp_folders = list()
            for f in all_file:
                if os.path.isdir(f) and f.startswith('temp_'):
                    temp_folders.append(f)
            tfile_new = list()
            for name in read_files(tfile):
                if name['temp_folder'] in temp_folders:
                    tfile_new.append(name)
            # 更新临时保存文件
            with open(tfile, 'w', encoding='UTF-8') as t:
                for name in tfile_new:
                    t.write(json.dumps(name) + '\n')
            print('[+] 更新临时文件:' + tfile)
        exit(0)
    except Exception as e:
        traceback.print_exc()
        print('[ERROR] ' + str(e))


if "__main__" == __name__:
    main()
