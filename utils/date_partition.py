from datetime import datetime, timedelta
import os
from typing import Optional

import pandas as pd

from utils.config import START_DATE, PARQUET_FILE_PERIOD, PARQUET_DIR, KLINE_INTERVAL


def get_available_years_months(period_month: int = 6):
    """
    获取从START_DATE至今的所有年份列表
    return:
    {
        "2021-01": ["2021-01", "2021-02", "2021-03", "2021-04", "2021-05", "2021-06"],
        "2021-07": ["2021-07", "2021-08", "2021-09", "2021-10", "2021-11", "2021-12"],
    }
    """
    assert 12 % period_month == 0, "period_month error"
    yms = []
    current_date = datetime.now()
    init_date = pd.to_datetime('2019-01-01')

    current = init_date.replace(day=1)  # 从月初开始

    while current <= current_date:
        yms.append(f'{current.year}-{current.month:02d}')
        # 移动到下个月
        if current.month + period_month > 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + period_month)

    first = f'{START_DATE.year}-{START_DATE.month:02d}'
    yms = [[ym for ym in yms if ym <= first][-1]] + [ym for ym in yms if ym > first]
    yms_dict = {}
    for ym in yms:
        month = int(ym.split('-')[1])
        yms_values = []
        for range_ym in range(month, month + period_month):
            yms_value = f'{ym.split('-')[0]}-{range_ym:02d}'
            if yms_value >= first:
                yms_values.append(yms_value)
        yms_dict[ym] = yms_values

    return yms_dict


def get_parquet_cutoff_date(
    current_date: Optional[datetime] = None,
    days_before: int = 30,
) -> Optional[datetime]:
    """
    计算parquet文件的截止日期

    截止日期的计算规则：
    1. 截止日期必须在当前日期的n天以前
    2. 按照当前parquet文件切割规则，这个截止日期之前的每一个parquet文件中的数据都已完整
    3. 后续无需再次追加数据

    Args:
        current_date: 当前日期，默认为今天
        days_before: 截止日期必须在当前日期的n天以前
        market: 市场类型（如'usdt_perp', 'usdt_spot'）
        interval: 时间间隔（默认'5m'）
        verify_files: 是否验证文件存在性

    Returns:
        datetime: 截止日期，如果没有可用的截止日期则返回None
    """
    if current_date is None:
        current_date = datetime.now()

    # 计算理论截止日期：当前日期减去n天
    theoretical_cutoff = current_date - pd.Timedelta(days=days_before)

    # 根据parquet文件切割规则调整到最近的完整边界
    cutoff_date = _adjust_to_parquet_boundary(theoretical_cutoff)

    return cutoff_date


def _adjust_to_parquet_boundary(date: datetime) -> datetime:
    """
    将日期调整到最近的parquet文件边界

    Args:
        date: 输入日期

    Returns:
        datetime: 调整后的日期
    """
    # 获取parquet文件周期（月）
    period_months = PARQUET_FILE_PERIOD

    # 计算从START_DATE开始的完整周期数
    months_since_start = (date.year - START_DATE.year) * 12 + (date.month - START_DATE.month)
    complete_periods = months_since_start // period_months

    # 计算截止日期：START_DATE + 完整周期数 * 周期月数
    cutoff_year = START_DATE.year + (complete_periods * period_months) // 12
    cutoff_month = START_DATE.month + (complete_periods * period_months) % 12

    # 处理月份超过12的情况
    if cutoff_month > 12:
        cutoff_year += 1
        cutoff_month -= 12

    return datetime(cutoff_year, cutoff_month, 1)


def get_latest_complete_parquet_file(
    given_date: datetime,
    market: str = 'usdt_perp',
    interval: str = '5m',
) -> Optional[dict]:
    """
    找到给定日期下应该被写入的最近一个完整的parquet文件

    Args:
        given_date: 给定的日期
        market: 市场类型（如'usdt_perp', 'usdt_spot'）
        interval: 时间间隔（默认'5m'）
        verify_files: 是否验证文件实际存在性

    Returns:
        dict: 包含文件信息的字典，格式为：
        {
            'filename': str,  # 文件名
            'period_start': str,  # 周期开始日期 (YYYY-MM)
            'period_end': str,    # 周期结束日期 (YYYY-MM)
            'period_months': int, # 周期月数
            'file_path': str,    # 完整文件路径
            'exists': bool       # 文件是否实际存在
        }
        如果没有找到完整的parquet文件则返回None
    """
    # 确保给定日期不早于START_DATE
    if given_date.date() < START_DATE:
        return None

    # 计算理论上的最近完整parquet文件
    theoretical_file = _calculate_theoretical_latest_file(given_date, market, interval)

    if theoretical_file is None:
        return None

    # 如果启用文件验证，检查文件是否实际存在
    file_path = os.path.join(PARQUET_DIR, f"{market}_{interval}", theoretical_file['filename'])
    theoretical_file['file_path'] = file_path
    theoretical_file['exists'] = os.path.exists(file_path)

    return theoretical_file


def _calculate_theoretical_latest_file(given_date: datetime, market: str, interval: str) -> Optional[dict]:
    """
    计算理论上的最近完整parquet文件

    Args:
        given_date: 给定日期
        market: 市场类型
        interval: 时间间隔

    Returns:
        dict: 文件信息字典，如果计算失败返回None
    """
    try:
        # 获取parquet文件周期（月）
        period_months = PARQUET_FILE_PERIOD

        # 计算从START_DATE到给定日期的完整周期数
        months_since_start = (given_date.year - START_DATE.year) * 12 + (given_date.month - START_DATE.month)
        complete_periods = months_since_start // period_months

        # 如果给定日期正好在周期边界上，使用前一个周期
        complete_periods -= 1
        if months_since_start % period_months == 0 and given_date.day == 1:
            complete_periods -= 1

        # 确保至少有一个完整周期
        if complete_periods < 0:
            return None

        # 计算最近完整周期的开始日期
        period_start_year = START_DATE.year + (complete_periods * period_months) // 12
        period_start_month = START_DATE.month + (complete_periods * period_months) % 12

        # 处理月份超过12的情况
        if period_start_month > 12:
            period_start_year += 1
            period_start_month -= 12

        # 计算周期结束日期
        period_end_year = period_start_year + (period_months - 1) // 12
        period_end_month = period_start_month + (period_months - 1) % 12

        # 处理结束月份超过12的情况
        if period_end_month > 12:
            period_end_year += 1
            period_end_month -= 12

        # 构建文件名
        period_start_str = f"{period_start_year}-{period_start_month:02d}"
        filename = f"{market}_{period_start_str}_{period_months}M.parquet"

        # 生成 YYYY-MM-DD 的周期起止日期
        start_date_str = f"{period_start_year}-{period_start_month:02d}-01"
        # 计算结束月的最后一天：下月1号减一天
        if period_end_month == 12:
            next_month_year = period_end_year + 1
            next_month_month = 1
        else:
            next_month_year = period_end_year
            next_month_month = period_end_month + 1
        last_day_dt = datetime(next_month_year, next_month_month, 1)
        end_date_str = last_day_dt.strftime('%Y-%m-%d')

        return {
            'filename': filename,
            'period_start': start_date_str,
            'period_end': end_date_str,
            'period_months': period_months,
            'file_path': '',  # 将在调用函数中设置
            'exists': None   # 将在调用函数中设置
        }

    except Exception:
        return None


if __name__ == '__main__':
    market = 'usdt_perp'
    test_date = datetime(2025, 5, 9)
    test_date = test_date - timedelta(days=7)
    # print(get_available_years_months(period_month=2))
    #
    # 测试get_parquet_cutoff_date函数
    print("\n测试get_parquet_cutoff_date函数:")
    cutoff = get_parquet_cutoff_date(current_date=test_date, days_before=30, market='usdt_perp')
    print(f"截止日期: {cutoff}")
    print(f"截止月份: {cutoff:%Y-%m}")

    latest_file2 = get_latest_complete_parquet_file(test_date, market=market)
    print(f"\n测试日期 {test_date.strftime('%Y-%m-%d')} 的最近完整parquet文件:")
    if latest_file2:
        print(f"  文件名: {latest_file2['filename']}")
        print(f"  周期开始: {latest_file2['period_start']}")
        print(f"  周期结束: {latest_file2['period_end']}")
        print(f"  文件存在: {latest_file2['exists']}")
    else:
        print("  未找到完整的parquet文件")

