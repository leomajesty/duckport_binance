"""
Download module for data center
Handles file downloading, validation and retry logic
"""

import asyncio
import os
import aiofiles
import aiohttp
import tqdm
from hashlib import sha256
from utils.log_kit import logger
from utils.config import (
    BASE_URL, semaphore, retry_times, thunder,
    file_proxy, need_analyse_set, daily_updated_set
)


def async_download_file(all_list, error_info_list):
    """Download all files with progress bar"""
    pbar = tqdm.tqdm(total=len(all_list), ncols=50, mininterval=0.5)
    tasks = download_file(all_list, pbar, error_info_list)
    asyncio.get_event_loop().run_until_complete(asyncio.gather(*tasks))
    pbar.close()


def download_file(params, pbar, error_info_list):
    """Create download tasks for all files"""
    tasks = []
    for param in params:
        key = param['key']
        download_checksum_url = f'{BASE_URL}{key}'
        sum_file_name = os.path.basename(param['key'])

        if not os.path.exists(param['local_path']):
            os.makedirs(param['local_path'])
        local_path = param['local_path']
        last_modified = param['last_modified']
        local_sum_path = os.path.join(local_path, sum_file_name)
        local_zip_path = os.path.join(local_path, sum_file_name[0:-9])
        
        if os.path.exists(local_sum_path):
            '''
            这里对checksum文件的更新时间与币安数据中心的更新时间作比较
            '''
            import datetime
            modify_utc_timestamp = datetime.datetime.utcfromtimestamp(os.path.getmtime(local_sum_path)).timestamp()
            if modify_utc_timestamp < last_modified:
                os.remove(local_sum_path)
                
        if os.path.exists(local_sum_path) and os.path.exists(local_zip_path):
            if not thunder:
                # 本地已有文件，进行校验
                with open(local_sum_path, encoding='utf-8') as in_sum_file:
                    correct_sum = in_sum_file.readline().split(' ')[0]
                sha256Obj = sha256()
                with open(local_zip_path, 'rb') as in_zip_file:
                    sha256Obj.update(in_zip_file.read())
                if correct_sum == sha256Obj.hexdigest().lower():
                    # logger.warning(local_zip_path, 'existed and is correct')
                    pbar.update(1)
                    continue  # 继续下一个zip的下载过程
            else:
                # 快速更新模式不校验本地已有文件
                pbar.update(1)
                continue  # 继续下一个zip的下载过程
                
        if 'monthly' in local_path:
            # 需要数据完整性分析的目录
            need_analyse_set.add(local_path)
        if 'daily_klines' in local_path:
            daily_updated_set.add(local_path)
            
        tasks.append(
            download(param['local_path'], download_checksum_url, local_sum_path, local_zip_path, pbar, error_info_list))
    return tasks


async def download(local_path, download_checksum_url, local_sum_path, local_zip_path, pbar, error_info_list):
    """Download single file with retry logic"""
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with semaphore:
            retry = 0
            while True:
                if retry > retry_times and 'daily_klines' in local_path:
                    logger.warning('下载daily zip失败次数超过retry_times，当前网络状况不稳定或数据包异常', local_zip_path)
                    break
                try:
                    sum_file = await session.get(download_checksum_url, proxy=file_proxy, timeout=20)
                    sum_file_buffer = await sum_file.read()
                    async with aiofiles.open(local_sum_path, 'wb') as out_sum_file:
                        await out_sum_file.write(sum_file_buffer)

                    zip_file = await session.get(download_checksum_url[0:-9], proxy=file_proxy, timeout=20)
                    zip_file_buffer = await zip_file.read()
                    async with aiofiles.open(local_zip_path, 'wb') as out_zip_file:
                        await out_zip_file.write(zip_file_buffer)

                    async with aiofiles.open(local_sum_path, encoding='utf-8') as in_sum_file:
                        str_sum = await in_sum_file.read()
                        correct_sum = str_sum.split(' ')[0]
                    sha256_obj = sha256()
                    async with aiofiles.open(local_zip_path, 'rb') as in_zip_file:
                        sha256_obj.update(await in_zip_file.read())
                    if correct_sum == sha256_obj.hexdigest().lower():
                        # logger.warning(local_zip_path, 'is correct')
                        pbar.update(1)
                        break
                except aiohttp.ClientError as ae:
                    error_info_list.add(f'下载{local_zip_path}失败，错误原因{ae}，已重试下载，请确认')
                except Exception as e:
                    error_info_list.add(f'下载{local_zip_path}失败，错误类型{type(e)}，已重试下载，请确认')
                retry += 1


async def download_miss_day_data(symbol, interval, day, local_path, sum_name, prefix, download_err_info):
    """Download missing day data with retry logic"""
    local_sum_path = os.path.join(local_path, sum_name)
    sum_url = BASE_URL + prefix + sum_name
    local_zip_path = local_sum_path[0:-9]
    err_times = 0
    retry_sum_404 = 0
    retry_zip_404 = 0
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with semaphore:
            while True:
                try:
                    sum_file = await session.get(sum_url, proxy=file_proxy, timeout=20)
                    if sum_file.status == 200:
                        sum_file_buffer = await sum_file.read()
                        async with aiofiles.open(local_sum_path, 'wb') as out_sum_file:
                            await out_sum_file.write(sum_file_buffer)
                    else:
                        retry_sum_404 += 1
                        if retry_sum_404 > 3:
                            # 重试3次确认不存在这个文件
                            break
                        else:
                            continue
                            
                    zip_file = await session.get(sum_url[0:-9], proxy=file_proxy, timeout=20)
                    if zip_file.status == 200:
                        zip_file_buffer = await zip_file.read()
                        async with aiofiles.open(local_zip_path, 'wb') as out_zip_file:
                            await out_zip_file.write(zip_file_buffer)
                    else:
                        retry_zip_404 += 1
                        if retry_zip_404 > 3:
                            # 重试3次确认不存在这个文件
                            break
                        else:
                            continue

                    async with aiofiles.open(local_sum_path, encoding='utf-8') as in_sum_file:
                        str_sum = await in_sum_file.readline()
                        correct_sum = str_sum.split(' ')[0]
                    sha256_obj = sha256()
                    async with aiofiles.open(local_zip_path, 'rb') as in_zip_file:
                        sha256_obj.update(await in_zip_file.read())
                    if correct_sum == sha256_obj.hexdigest().lower():
                        # logger.info(local_zip_path, 'is correct')
                        break
                except aiohttp.ClientError as ae:
                    download_err_info.add(f'下载{local_zip_path}失败，错误原因{ae}，已重试下载，请确认')
                    err_times += 1
                    if err_times > 5:
                        logger.error('补漏下载重试超过5次')
                        raise ae
                except Exception as e:
                    logger.info(f'下载{local_zip_path}失败，错误类型{type(e)}，已重试下载，请确认')
                    download_err_info.add(f'下载{local_zip_path}失败，错误类型{type(e)}，已重试下载，请确认')
                    err_times += 1
                    if err_times > 5:
                        logger.error('补漏下载重试超过5次')
                        raise e
                        
    if retry_sum_404 > 3 or retry_zip_404 > 3:
        logger.info(f'{sum_name} not exist, request from api')
        # Note: This function would need to be implemented if API fallback is required
        # For now, we'll just log the missing data
        logger.warning(f'Missing data for {symbol} on {day}, API fallback not implemented in simplified version')
