"""
DuckDB 并发控制演示
对比 MySQL 连接池的概念，为 DuckDB 实现类似的并发限制
"""
import threading
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import duckdb
from utils.log_kit import logger


class DuckDBWithoutConcurrencyControl:
    """不带并发控制的 DuckDB 管理器（可能导致资源耗尽）"""

    def __init__(self, db_path: str = "test_performance.duckdb"):
        self.db_path = db_path
        self._connection = None
        self._thread_local = threading.local()
        self._init_connection()

    def _init_connection(self):
        config = {'threads': 4, 'memory_limit': '2GB'}
        self._connection = duckdb.connect(database=self.db_path, read_only=False, config=config)

    def _get_cursor(self):
        if not hasattr(self._thread_local, 'cursor'):
            self._thread_local.cursor = self._connection.cursor()
            logger.debug(f"创建 cursor: {threading.current_thread().name}")
        return self._thread_local.cursor

    def execute_query(self, query: str):
        """无并发限制的查询"""
        cursor = self._get_cursor()
        thread_name = threading.current_thread().name
        logger.info(f"[{thread_name}] 开始查询...")

        start = time.time()
        result = cursor.execute(query).fetchall()
        elapsed = time.time() - start

        logger.info(f"[{thread_name}] 查询完成，耗时 {elapsed:.3f}秒")
        return result

    def close(self):
        if self._connection:
            self._connection.close()


class DuckDBWithSemaphore:
    """带 Semaphore 并发控制的 DuckDB 管理器（类似 MySQL 连接池）"""

    def __init__(self, db_path: str = "test_performance.duckdb", max_concurrent: int = 5):
        self.db_path = db_path
        self.max_concurrent = max_concurrent
        self._connection = None
        self._thread_local = threading.local()
        self._semaphore = threading.Semaphore(max_concurrent)  # 类似连接池的并发限制
        self._active_queries = 0
        self._lock = threading.Lock()
        self._init_connection()

    def _init_connection(self):
        config = {'threads': 4, 'memory_limit': '2GB'}
        self._connection = duckdb.connect(database=self.db_path, read_only=False, config=config)
        logger.info(f"✓ DuckDB 初始化，最大并发: {self.max_concurrent}")

    def _get_cursor(self):
        if not hasattr(self._thread_local, 'cursor'):
            self._thread_local.cursor = self._connection.cursor()
            logger.debug(f"创建 cursor: {threading.current_thread().name}")
        return self._thread_local.cursor

    def execute_query(self, query: str):
        """带并发控制的查询（类似从连接池获取连接）"""
        thread_name = threading.current_thread().name

        # 等待获取"连接"（实际是并发槽位）
        logger.info(f"[{thread_name}] 等待并发槽位... (当前活跃: {self._active_queries}/{self.max_concurrent})")

        with self._semaphore:  # 类似 connection_pool.get_connection()
            with self._lock:
                self._active_queries += 1
                current_active = self._active_queries

            logger.info(f"[{thread_name}] ✓ 获得槽位，开始查询 (活跃: {current_active}/{self.max_concurrent})")

            cursor = self._get_cursor()
            start = time.time()
            result = cursor.execute(query).fetchall()
            elapsed = time.time() - start

            with self._lock:
                self._active_queries -= 1
                current_active = self._active_queries

            logger.info(f"[{thread_name}] 查询完成，耗时 {elapsed:.3f}秒，释放槽位 (活跃: {current_active}/{self.max_concurrent})")
            return result

    def close(self):
        if self._connection:
            self._connection.close()


class AsyncDuckDBWithSemaphore:
    """异步版本：带 asyncio.Semaphore 的 DuckDB 管理器"""

    def __init__(self, db_path: str = "test_performance.duckdb", max_concurrent: int = 5):
        self.db_path = db_path
        self.max_concurrent = max_concurrent
        self._connection = None
        self._semaphore = None  # 在 async 环境中初始化
        self._executor = ThreadPoolExecutor(max_workers=max_concurrent)
        self._init_connection()

    def _init_connection(self):
        config = {'threads': 4, 'memory_limit': '2GB'}
        self._connection = duckdb.connect(database=self.db_path, read_only=False, config=config)
        logger.info(f"✓ 异步 DuckDB 初始化，最大并发: {self.max_concurrent}")

    async def execute_query(self, query: str, task_id: int):
        """异步查询（带并发控制）"""
        if self._semaphore is None:
            self._semaphore = asyncio.Semaphore(self.max_concurrent)

        logger.info(f"[Task-{task_id}] 等待并发槽位...")

        async with self._semaphore:
            logger.info(f"[Task-{task_id}] ✓ 获得槽位，开始查询")

            # 在线程池中执行同步的 DuckDB 查询
            loop = asyncio.get_event_loop()
            start = time.time()
            result = await loop.run_in_executor(
                self._executor,
                lambda: self._connection.cursor().execute(query).fetchall()
            )
            elapsed = time.time() - start

            logger.info(f"[Task-{task_id}] 查询完成，耗时 {elapsed:.3f}秒")
            return result

    def close(self):
        if self._connection:
            self._connection.close()
        self._executor.shutdown(wait=True)


def create_test_table(manager):
    """创建测试表并插入数据"""
    # 兼容同步和异步管理器
    if hasattr(manager, '_get_cursor'):
        cursor = manager._get_cursor()
    else:
        cursor = manager._connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS concurrency_test")
    cursor.execute("""
        CREATE TABLE concurrency_test (
            id INTEGER,
            value DOUBLE,
            timestamp TIMESTAMP
        )
    """)

    # 插入测试数据
    for i in range(1000):
        cursor.execute(
            "INSERT INTO concurrency_test VALUES (?, ?, ?)",
            (i, i * 10.0, datetime.now())
        )

    logger.info("✓ 测试表创建完成，已插入 1000 条数据\n")


def test_without_concurrency_control():
    """
    测试1: 不带并发控制
    所有线程同时执行，可能导致资源竞争
    """
    logger.info("="*80)
    logger.info("测试1: 不带并发控制（20个并发查询）")
    logger.info("="*80 + "\n")

    manager = DuckDBWithoutConcurrencyControl()
    create_test_table(manager)

    # 模拟重查询
    query = """
        SELECT COUNT(*), AVG(value), SUM(value), MAX(value), MIN(value)
        FROM concurrency_test
        WHERE value > 100
    """

    start_time = time.time()

    # 启动 20 个并发查询
    with ThreadPoolExecutor(max_workers=20, thread_name_prefix="Query") as executor:
        futures = [executor.submit(manager.execute_query, query) for i in range(20)]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"查询失败: {e}")

    total_time = time.time() - start_time

    logger.info(f"\n{'='*80}")
    logger.info(f"不带并发控制总耗时: {total_time:.3f}秒")
    logger.info(f"{'='*80}\n")

    manager.close()
    return total_time


def test_with_semaphore():
    """
    测试2: 带 Semaphore 并发控制
    限制同时执行的查询数量，类似 MySQL 连接池
    """
    logger.info("="*80)
    logger.info("测试2: 带 Semaphore 并发控制（最大5个并发）")
    logger.info("类似 MySQL 连接池限制连接数")
    logger.info("="*80 + "\n")

    manager = DuckDBWithSemaphore(max_concurrent=5)
    create_test_table(manager)

    query = """
        SELECT COUNT(*), AVG(value), SUM(value), MAX(value), MIN(value)
        FROM concurrency_test
        WHERE value > 100
    """

    start_time = time.time()

    # 启动 20 个查询请求，但最多同时执行 5 个
    with ThreadPoolExecutor(max_workers=20, thread_name_prefix="Query") as executor:
        futures = [executor.submit(manager.execute_query, query) for i in range(20)]

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"查询失败: {e}")

    total_time = time.time() - start_time

    logger.info(f"\n{'='*80}")
    logger.info(f"带并发控制总耗时: {total_time:.3f}秒")
    logger.info(f"{'='*80}\n")

    manager.close()
    return total_time


async def test_async_with_semaphore():
    """
    测试3: 异步 + Semaphore 并发控制
    最灵活的方式，适合高并发场景
    """
    logger.info("="*80)
    logger.info("测试3: 异步 + Semaphore 并发控制")
    logger.info("="*80 + "\n")

    manager = AsyncDuckDBWithSemaphore(max_concurrent=5)
    create_test_table(manager)

    query = """
        SELECT COUNT(*), AVG(value), SUM(value), MAX(value), MIN(value)
        FROM concurrency_test
        WHERE value > 100
    """

    start_time = time.time()

    # 启动 20 个异步查询
    tasks = [manager.execute_query(query, i) for i in range(20)]
    await asyncio.gather(*tasks)

    total_time = time.time() - start_time

    logger.info(f"\n{'='*80}")
    logger.info(f"异步并发控制总耗时: {total_time:.3f}秒")
    logger.info(f"{'='*80}\n")

    manager.close()
    return total_time


def main():
    """主测试函数"""
    logger.info("="*80)
    logger.info("DuckDB 并发控制对比测试")
    logger.info("对比 MySQL 连接池的概念")
    logger.info("="*80 + "\n")

    # 测试1: 无并发控制
    time1 = test_without_concurrency_control()

    # 测试2: 带 Semaphore
    time2 = test_with_semaphore()

    # 测试3: 异步 + Semaphore
    time3 = asyncio.run(test_async_with_semaphore())

    # 总结
    logger.info("="*80)
    logger.info("性能总结")
    logger.info("="*80)
    logger.info(f"1. 无并发控制:      {time1:.3f}秒")
    logger.info(f"2. Semaphore 控制:  {time2:.3f}秒")
    logger.info(f"3. 异步 Semaphore:  {time3:.3f}秒")
    logger.info("\n结论:")
    logger.info("  ✓ 虽然 DuckDB 是嵌入式数据库，不需要传统的连接池")
    logger.info("  ✓ 但在高并发场景下，仍然需要 Semaphore 控制并发数")
    logger.info("  ✓ 原因: 防止过多并发导致内存/CPU 资源耗尽")
    logger.info("  ✓ 类比: MySQL 连接池限制连接数 → DuckDB Semaphore 限制并发查询数")


if __name__ == "__main__":
    main()
