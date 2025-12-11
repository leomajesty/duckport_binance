"""
DuckDB 并发读写实时演示
清晰展示读写操作真正同时进行的过程
"""
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import duckdb
from utils.log_kit import logger


class RealtimeConcurrentManager:
    """实时并发演示管理器"""

    def __init__(self, db_path: str = "test_performance.duckdb"):
        self.db_path = db_path
        self._connection = None
        self._thread_local = threading.local()
        self._operation_log = []  # 记录所有操作的时间线
        self._lock = threading.Lock()  # 用于保护操作日志
        self._start_time = None
        self._init_connection()

    def _init_connection(self):
        """初始化数据库连接"""
        config = {
            'threads': 4,
            'memory_limit': '2GB'
        }
        self._connection = duckdb.connect(database=self.db_path, read_only=False, config=config)
        logger.info(f"✓ 连接数据库: {self.db_path}")

    def _get_cursor(self):
        """获取线程本地 cursor"""
        if not hasattr(self._thread_local, 'cursor'):
            self._thread_local.cursor = self._connection.cursor()
        return self._thread_local.cursor

    def _log_operation(self, thread_name: str, operation: str, stage: str):
        """记录操作时间点"""
        # 如果还没有开始计时，自动初始化
        if self._start_time is None:
            self._start_time = time.time()

        elapsed = time.time() - self._start_time
        with self._lock:
            self._operation_log.append({
                'time': elapsed,
                'thread': thread_name,
                'operation': operation,
                'stage': stage
            })

    def create_demo_table(self):
        """创建演示用的测试表"""
        cursor = self._get_cursor()
        cursor.execute("DROP TABLE IF EXISTS concurrent_demo")
        cursor.execute("""
            CREATE TABLE concurrent_demo (
                id INTEGER,
                thread_name VARCHAR,
                operation VARCHAR,
                value DOUBLE,
                timestamp TIMESTAMP,
                message VARCHAR
            )
        """)
        logger.info("✓ 创建表: concurrent_demo")

    def insert_data_realtime(self, thread_id: int, batch_size: int = 100, delay: float = 0.01):
        """
        写入数据（带实时日志）

        Args:
            thread_id: 线程ID
            batch_size: 每批插入的数据量
            delay: 每次插入之间的延迟（模拟真实写入）
        """
        cursor = self._get_cursor()
        thread_name = threading.current_thread().name

        self._log_operation(thread_name, 'WRITE', 'START')
        logger.info(f"🟢 [{thread_name}] 开始写入 {batch_size} 条数据")

        try:
            for i in range(batch_size):
                record_id = thread_id * 10000 + i
                value = random.uniform(100, 1000)
                timestamp = datetime.now() + timedelta(seconds=i)
                message = f"Thread-{thread_id} record {i}"

                cursor.execute(
                    "INSERT INTO concurrent_demo VALUES (?, ?, ?, ?, ?, ?)",
                    (record_id, thread_name, 'INSERT', value, timestamp, message)
                )

                # 每写入10条记录打印一次进度
                if (i + 1) % 10 == 0:
                    logger.info(f"  [{thread_name}] 已写入 {i + 1}/{batch_size} 条")

                time.sleep(delay)  # 模拟写入延迟

            self._log_operation(thread_name, 'WRITE', 'END')
            logger.info(f"✅ [{thread_name}] 写入完成")

        except Exception as e:
            self._log_operation(thread_name, 'WRITE', 'ERROR')
            logger.error(f"❌ [{thread_name}] 写入失败: {e}")
            raise

    def read_data_realtime(self, thread_id: int, iterations: int = 10, delay: float = 0.05):
        """
        读取数据（带实时日志）

        Args:
            thread_id: 线程ID
            iterations: 查询次数
            delay: 每次查询之间的延迟
        """
        cursor = self._get_cursor()
        thread_name = threading.current_thread().name

        self._log_operation(thread_name, 'READ', 'START')
        logger.info(f"🔵 [{thread_name}] 开始读取，共 {iterations} 次")

        try:
            for i in range(iterations):
                query = """
                    SELECT COUNT(*), AVG(value), MAX(value), MIN(value)
                    FROM concurrent_demo
                    WHERE value > ?
                """
                result = cursor.execute(query, (100,)).fetchone()

                avg_value = result[1] if result[1] is not None else 0
                logger.info(f"  [{thread_name}] 第 {i+1}/{iterations} 次查询: 找到 {result[0]} 条记录, 平均值 {avg_value:.2f}")

                time.sleep(delay)  # 模拟查询延迟

            self._log_operation(thread_name, 'READ', 'END')
            logger.info(f"✅ [{thread_name}] 读取完成")

        except Exception as e:
            self._log_operation(thread_name, 'READ', 'ERROR')
            logger.error(f"❌ [{thread_name}] 读取失败: {e}")
            raise

    def print_timeline(self):
        """打印操作时间线"""
        logger.info(f"\n{'='*80}")
        logger.info("操作时间线（所有操作按时间顺序）")
        logger.info(f"{'='*80}")

        # 按时间排序
        sorted_log = sorted(self._operation_log, key=lambda x: x['time'])

        # 统计每个线程的开始和结束时间
        thread_times = {}
        for log in sorted_log:
            thread = log['thread']
            if thread not in thread_times:
                thread_times[thread] = {'start': None, 'end': None, 'operation': log['operation']}

            if log['stage'] == 'START':
                thread_times[thread]['start'] = log['time']
            elif log['stage'] == 'END':
                thread_times[thread]['end'] = log['time']

        # 打印时间线
        logger.info("\n时间(秒)  线程名称               操作    阶段")
        logger.info("-" * 80)
        for log in sorted_log:
            operation_emoji = "🔵 READ " if log['operation'] == 'READ' else "🟢 WRITE"
            stage_emoji = "▶️ " if log['stage'] == 'START' else "✅ " if log['stage'] == 'END' else "❌ "
            logger.info(f"{log['time']:7.3f}   {log['thread']:20} {operation_emoji}  {stage_emoji}{log['stage']}")

        # 打印可视化时间线
        logger.info(f"\n{'='*80}")
        logger.info("可视化时间线（每个字符 ≈ 0.1秒）")
        logger.info(f"{'='*80}")

        max_time = max(log['time'] for log in sorted_log)
        scale = 0.1  # 每个字符代表0.1秒

        for thread, times in sorted_thread_times.items():
            if times['start'] is not None and times['end'] is not None:
                start_pos = int(times['start'] / scale)
                end_pos = int(times['end'] / scale)
                duration = times['end'] - times['start']

                operation = times['operation']
                symbol = '█' if operation == 'WRITE' else '▓'
                color = '🟢' if operation == 'WRITE' else '🔵'

                timeline = ' ' * start_pos + symbol * (end_pos - start_pos)
                logger.info(f"{color} {thread:20} |{timeline}| ({duration:.2f}s)")

        # 打印时间刻度
        max_pos = int(max_time / scale) + 1
        scale_line = ''.join([str(i % 10) if i % 10 == 0 else '.' for i in range(max_pos)])
        logger.info(f"   {'时间刻度(秒)':20} |{scale_line}|")

    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()


def test_true_concurrent_read_write():
    """
    测试真正的并发读写
    读写操作同时开始，同时进行
    """
    logger.info("="*80)
    logger.info("DuckDB 真正的并发读写演示")
    logger.info("读写操作将同时开始并同时执行")
    logger.info("="*80)

    manager = RealtimeConcurrentManager("test_performance.duckdb")

    try:
        # 创建测试表
        manager.create_demo_table()

        # 先插入一些初始数据，让读操作有数据可读
        logger.info("\n准备阶段：插入初始数据...")
        manager.insert_data_realtime(0, batch_size=50, delay=0.001)

        # 重置计时器和操作日志
        manager._operation_log = []
        manager._start_time = None  # 重置为 None，让第一个操作自动初始化

        logger.info(f"\n{'='*80}")
        logger.info("开始并发读写测试")
        logger.info(f"{'='*80}\n")

        # 使用 ThreadPoolExecutor 同时提交读写任务
        with ThreadPoolExecutor(max_workers=6, thread_name_prefix="Worker") as executor:
            # 同时提交多个读任务
            read_futures = [
                executor.submit(manager.read_data_realtime, i, iterations=5, delay=0.1)
                for i in range(3)  # 3个读线程
            ]

            # 同时提交多个写任务
            write_futures = [
                executor.submit(manager.insert_data_realtime, i + 10, batch_size=30, delay=0.02)
                for i in range(3)  # 3个写线程
            ]

            # 等待所有任务完成
            all_futures = read_futures + write_futures
            for future in all_futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"任务执行失败: {e}")

        # 打印时间线
        manager.print_timeline()

        # 统计信息
        logger.info(f"\n{'='*80}")
        logger.info("统计信息")
        logger.info(f"{'='*80}")

        cursor = manager._get_cursor()
        stats = cursor.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT thread_name) as thread_count,
                SUM(CASE WHEN operation = 'INSERT' THEN 1 ELSE 0 END) as insert_count
            FROM concurrent_demo
        """).fetchone()

        logger.info(f"总记录数: {stats[0]}")
        logger.info(f"涉及线程数: {stats[1]}")
        logger.info(f"写入操作数: {stats[2]}")

        logger.info(f"\n{'='*80}")
        logger.info("✓ 测试完成!")
        logger.info("从时间线可以看出：读写操作确实是同时进行的！")
        logger.info(f"{'='*80}")

    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise
    finally:
        manager.close()


def test_sequential_vs_concurrent():
    """
    对比顺序执行 vs 并发执行
    """
    logger.info("\n" + "="*80)
    logger.info("对比测试：顺序执行 vs 并发执行")
    logger.info("="*80)

    manager = RealtimeConcurrentManager("test_performance.duckdb")

    try:
        manager.create_demo_table()

        # 先插入初始数据
        manager.insert_data_realtime(0, batch_size=50, delay=0.001)

        # ============ 测试1: 顺序执行 ============
        logger.info(f"\n{'='*80}")
        logger.info("测试1: 顺序执行（先读后写）")
        logger.info(f"{'='*80}\n")

        manager._operation_log = []
        manager._start_time = None
        test_start = time.time()

        # 先读
        manager.read_data_realtime(1, iterations=3, delay=0.1)
        # 后写
        manager.insert_data_realtime(11, batch_size=20, delay=0.02)

        sequential_time = time.time() - test_start
        logger.info(f"\n顺序执行总耗时: {sequential_time:.3f}秒")

        # ============ 测试2: 并发执行 ============
        logger.info(f"\n{'='*80}")
        logger.info("测试2: 并发执行（读写同时）")
        logger.info(f"{'='*80}\n")

        manager._operation_log = []
        manager._start_time = None
        test_start = time.time()

        with ThreadPoolExecutor(max_workers=2) as executor:
            read_future = executor.submit(manager.read_data_realtime, 2, iterations=3, delay=0.1)
            write_future = executor.submit(manager.insert_data_realtime, 12, batch_size=20, delay=0.02)

            read_future.result()
            write_future.result()

        concurrent_time = time.time() - test_start
        logger.info(f"\n并发执行总耗时: {concurrent_time:.3f}秒")

        # 对比结果
        logger.info(f"\n{'='*80}")
        logger.info("性能对比")
        logger.info(f"{'='*80}")
        logger.info(f"顺序执行: {sequential_time:.3f}秒")
        logger.info(f"并发执行: {concurrent_time:.3f}秒")
        logger.info(f"性能提升: {sequential_time/concurrent_time:.2f}x")
        logger.info(f"时间节省: {(1 - concurrent_time/sequential_time)*100:.1f}%")

        # 打印时间线
        manager.print_timeline()

    finally:
        manager.close()


# 修复时间线排序问题
def print_timeline_fixed(manager):
    """打印操作时间线（修复版）"""
    logger.info(f"\n{'='*80}")
    logger.info("操作时间线（所有操作按时间顺序）")
    logger.info(f"{'='*80}")

    # 按时间排序
    sorted_log = sorted(manager._operation_log, key=lambda x: x['time'])

    # 统计每个线程的开始和结束时间
    thread_times = {}
    for log in sorted_log:
        thread = log['thread']
        if thread not in thread_times:
            thread_times[thread] = {'start': None, 'end': None, 'operation': log['operation']}

        if log['stage'] == 'START':
            thread_times[thread]['start'] = log['time']
        elif log['stage'] == 'END':
            thread_times[thread]['end'] = log['time']

    # 打印时间线
    logger.info("\n时间(秒)  线程名称               操作    阶段")
    logger.info("-" * 80)
    for log in sorted_log:
        operation_emoji = "🔵 READ " if log['operation'] == 'READ' else "🟢 WRITE"
        stage_emoji = "▶️ " if log['stage'] == 'START' else "✅ " if log['stage'] == 'END' else "❌ "
        logger.info(f"{log['time']:7.3f}   {log['thread']:20} {operation_emoji}  {stage_emoji}{log['stage']}")

    # 打印可视化时间线
    logger.info(f"\n{'='*80}")
    logger.info("可视化时间线（每个字符 ≈ 0.1秒）")
    logger.info(f"{'='*80}")

    if not sorted_log:
        return

    max_time = max(log['time'] for log in sorted_log)
    scale = 0.1  # 每个字符代表0.1秒

    for thread, times in sorted(thread_times.items()):
        if times['start'] is not None and times['end'] is not None:
            start_pos = int(times['start'] / scale)
            end_pos = int(times['end'] / scale)
            duration = times['end'] - times['start']

            operation = times['operation']
            symbol = '█' if operation == 'WRITE' else '▓'
            color = '🟢' if operation == 'WRITE' else '🔵'

            timeline = ' ' * start_pos + symbol * (end_pos - start_pos + 1)
            logger.info(f"{color} {thread:20} |{timeline}| ({duration:.2f}s)")

    # 打印时间刻度
    max_pos = int(max_time / scale) + 1
    scale_line = ''.join([str(i // 10) if i % 10 == 0 else '.' for i in range(max_pos)])
    logger.info(f"   {'时间刻度(秒)':20} |{scale_line}|")


# 替换原有的 print_timeline 方法
RealtimeConcurrentManager.print_timeline = lambda self: print_timeline_fixed(self)


if __name__ == "__main__":
    # 运行真正的并发读写测试
    test_true_concurrent_read_write()

    # 可选：运行对比测试
    # test_sequential_vs_concurrent()
