"""
DuckDB 并发读写演示
展示 DuckDB 在多线程环境下的并发能力
"""
import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import duckdb
from utils.log_kit import logger


class ConcurrentDemoManager:
    """并发读写演示管理器"""

    def __init__(self, db_path: str = "test_performance.duckdb"):
        self.db_path = db_path
        self._connection = None
        self._thread_local = threading.local()
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
        """获取线程本地 cursor（每个线程独立的连接）"""
        if not hasattr(self._thread_local, 'cursor'):
            self._thread_local.cursor = self._connection.cursor()
            thread_name = threading.current_thread().name
            logger.debug(f"为线程 {thread_name} 创建新 cursor")
        return self._thread_local.cursor

    def create_demo_table(self):
        """创建演示用的测试表"""
        cursor = self._get_cursor()

        # 删除旧表（如果存在）
        cursor.execute("DROP TABLE IF EXISTS concurrent_demo")

        # 创建新表
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

    def insert_data(self, thread_id: int, batch_size: int = 100):
        """
        插入数据（模拟写操作）

        Args:
            thread_id: 线程ID
            batch_size: 每批插入的数据量
        """
        cursor = self._get_cursor()
        thread_name = threading.current_thread().name
        start_time = time.time()

        try:
            # 批量插入数据
            values = []
            for i in range(batch_size):
                record_id = thread_id * 10000 + i
                value = random.uniform(100, 1000)
                timestamp = datetime.now() + timedelta(seconds=i)
                message = f"Thread-{thread_id} batch insert"

                values.append((record_id, thread_name, 'INSERT', value, timestamp, message))

            cursor.executemany(
                "INSERT INTO concurrent_demo VALUES (?, ?, ?, ?, ?, ?)",
                values
            )

            elapsed = time.time() - start_time
            logger.info(f"✓ [{thread_name}] 写入 {batch_size} 条数据，耗时 {elapsed:.3f}秒")
            return {'thread_id': thread_id, 'operation': 'INSERT', 'count': batch_size, 'time': elapsed}

        except Exception as e:
            logger.error(f"✗ [{thread_name}] 写入失败: {e}")
            raise

    def read_data(self, thread_id: int, limit: int = 100):
        """
        读取数据（模拟读操作）

        Args:
            thread_id: 线程ID
            limit: 读取数据量
        """
        cursor = self._get_cursor()
        thread_name = threading.current_thread().name
        start_time = time.time()

        try:
            # 随机查询数据
            query = f"""
                SELECT id, thread_name, operation, value, timestamp, message
                FROM concurrent_demo
                WHERE value > ?
                ORDER BY timestamp DESC
                LIMIT {limit}
            """
            result = cursor.execute(query, (random.uniform(0, 500),)).fetchall()

            elapsed = time.time() - start_time
            logger.info(f"✓ [{thread_name}] 查询到 {len(result)} 条数据，耗时 {elapsed:.3f}秒")
            return {'thread_id': thread_id, 'operation': 'SELECT', 'count': len(result), 'time': elapsed}

        except Exception as e:
            logger.error(f"✗ [{thread_name}] 查询失败: {e}")
            raise

    def update_data(self, thread_id: int, batch_size: int = 50):
        """
        更新数据（模拟写操作）

        Args:
            thread_id: 线程ID
            batch_size: 更新数据量
        """
        cursor = self._get_cursor()
        thread_name = threading.current_thread().name
        start_time = time.time()

        try:
            # 更新随机数据
            query = """
                UPDATE concurrent_demo
                SET value = value * 1.1,
                    message = message || ' [UPDATED]'
                WHERE id % ? = ?
            """
            cursor.execute(query, (10, thread_id % 10))

            elapsed = time.time() - start_time
            logger.info(f"✓ [{thread_name}] 更新数据完成，耗时 {elapsed:.3f}秒")
            return {'thread_id': thread_id, 'operation': 'UPDATE', 'count': batch_size, 'time': elapsed}

        except Exception as e:
            logger.error(f"✗ [{thread_name}] 更新失败: {e}")
            raise

    def get_table_stats(self):
        """获取表统计信息"""
        cursor = self._get_cursor()

        result = cursor.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT thread_name) as thread_count,
                MIN(timestamp) as first_record,
                MAX(timestamp) as last_record,
                AVG(value) as avg_value,
                MIN(value) as min_value,
                MAX(value) as max_value
            FROM concurrent_demo
        """).fetchone()

        return result

    def close(self):
        """关闭数据库连接"""
        if self._connection:
            self._connection.close()
            logger.info("✓ 数据库连接已关闭")


def test_concurrent_write(manager: ConcurrentDemoManager, num_threads: int = 5, batch_size: int = 200):
    """
    测试并发写入

    Args:
        manager: 数据库管理器
        num_threads: 并发线程数
        batch_size: 每线程写入数据量
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"测试1: 并发写入 ({num_threads} 个线程)")
    logger.info(f"{'='*60}")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="Writer") as executor:
        futures = [executor.submit(manager.insert_data, i, batch_size) for i in range(num_threads)]

        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"线程执行失败: {e}")

    total_time = time.time() - start_time
    total_records = sum(r['count'] for r in results)

    logger.info(f"\n✓ 并发写入完成:")
    logger.info(f"  - 总线程数: {num_threads}")
    logger.info(f"  - 总记录数: {total_records}")
    logger.info(f"  - 总耗时: {total_time:.3f}秒")
    logger.info(f"  - 吞吐量: {total_records/total_time:.0f} 条/秒")

    return {
        'total_time': total_time,
        'total_records': total_records,
        'throughput': total_records/total_time
    }


def test_concurrent_read(manager: ConcurrentDemoManager, num_threads: int = 10, limit: int = 100):
    """
    测试并发读取

    Args:
        manager: 数据库管理器
        num_threads: 并发线程数
        limit: 每线程读取数据量
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"测试2: 并发读取 ({num_threads} 个线程)")
    logger.info(f"{'='*60}")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads, thread_name_prefix="Reader") as executor:
        futures = [executor.submit(manager.read_data, i, limit) for i in range(num_threads)]

        results = []
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"线程执行失败: {e}")

    total_time = time.time() - start_time
    total_records = sum(r['count'] for r in results)

    logger.info(f"\n✓ 并发读取完成:")
    logger.info(f"  - 总线程数: {num_threads}")
    logger.info(f"  - 总查询数: {total_records}")
    logger.info(f"  - 总耗时: {total_time:.3f}秒")
    logger.info(f"  - 吞吐量: {total_records/total_time:.0f} 条/秒")

    return {
        'total_time': total_time,
        'total_records': total_records,
        'throughput': total_records/total_time
    }


def test_concurrent_read_write(manager: ConcurrentDemoManager, num_readers: int = 8, num_writers: int = 2):
    """
    测试同时读写

    Args:
        manager: 数据库管理器
        num_readers: 读线程数
        num_writers: 写线程数
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"测试3: 并发读写 ({num_readers} 读线程 + {num_writers} 写线程)")
    logger.info(f"{'='*60}")

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_readers + num_writers) as executor:
        # 提交读任务
        read_futures = [
            executor.submit(manager.read_data, i, 100)
            for i in range(num_readers)
        ]

        # 提交写任务
        write_futures = [
            executor.submit(manager.insert_data, i + 100, 150)
            for i in range(num_writers)
        ]

        # 收集结果
        all_futures = read_futures + write_futures
        results = []
        for future in as_completed(all_futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"线程执行失败: {e}")

    total_time = time.time() - start_time
    read_results = [r for r in results if r['operation'] == 'SELECT']
    write_results = [r for r in results if r['operation'] == 'INSERT']

    logger.info(f"\n✓ 并发读写完成:")
    logger.info(f"  - 读操作: {len(read_results)} 个线程")
    logger.info(f"  - 写操作: {len(write_results)} 个线程")
    logger.info(f"  - 总耗时: {total_time:.3f}秒")

    return {
        'total_time': total_time,
        'read_count': len(read_results),
        'write_count': len(write_results),
        'total_operations': len(results)
    }


# ============ 单线程对照组 ============

def test_single_thread_write(manager: ConcurrentDemoManager, num_tasks: int = 5, batch_size: int = 200):
    """
    单线程写入对照组

    Args:
        manager: 数据库管理器
        num_tasks: 任务数（对应多线程的线程数）
        batch_size: 每次写入数据量
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"对照组1: 单线程写入 ({num_tasks} 次操作)")
    logger.info(f"{'='*60}")

    start_time = time.time()
    results = []

    for i in range(num_tasks):
        try:
            result = manager.insert_data(i, batch_size)
            results.append(result)
        except Exception as e:
            logger.error(f"写入失败: {e}")

    total_time = time.time() - start_time
    total_records = sum(r['count'] for r in results)

    logger.info(f"\n✓ 单线程写入完成:")
    logger.info(f"  - 总任务数: {num_tasks}")
    logger.info(f"  - 总记录数: {total_records}")
    logger.info(f"  - 总耗时: {total_time:.3f}秒")
    logger.info(f"  - 吞吐量: {total_records/total_time:.0f} 条/秒")

    return {
        'total_time': total_time,
        'total_records': total_records,
        'throughput': total_records/total_time
    }


def test_single_thread_read(manager: ConcurrentDemoManager, num_tasks: int = 10, limit: int = 100):
    """
    单线程读取对照组

    Args:
        manager: 数据库管理器
        num_tasks: 任务数（对应多线程的线程数）
        limit: 每次读取数据量
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"对照组2: 单线程读取 ({num_tasks} 次操作)")
    logger.info(f"{'='*60}")

    start_time = time.time()
    results = []

    for i in range(num_tasks):
        try:
            result = manager.read_data(i, limit)
            results.append(result)
        except Exception as e:
            logger.error(f"查询失败: {e}")

    total_time = time.time() - start_time
    total_records = sum(r['count'] for r in results)

    logger.info(f"\n✓ 单线程读取完成:")
    logger.info(f"  - 总任务数: {num_tasks}")
    logger.info(f"  - 总查询数: {total_records}")
    logger.info(f"  - 总耗时: {total_time:.3f}秒")
    logger.info(f"  - 吞吐量: {total_records/total_time:.0f} 条/秒")

    return {
        'total_time': total_time,
        'total_records': total_records,
        'throughput': total_records/total_time
    }


def test_single_thread_read_write(manager: ConcurrentDemoManager, num_readers: int = 8, num_writers: int = 2):
    """
    单线程读写对照组

    Args:
        manager: 数据库管理器
        num_readers: 读任务数
        num_writers: 写任务数
    """
    logger.info(f"\n{'='*60}")
    logger.info(f"对照组3: 单线程读写 ({num_readers} 次读 + {num_writers} 次写)")
    logger.info(f"{'='*60}")

    start_time = time.time()
    results = []

    # 先执行写操作
    for i in range(num_writers):
        try:
            result = manager.insert_data(i + 200, 150)
            results.append(result)
        except Exception as e:
            logger.error(f"写入失败: {e}")

    # 再执行读操作
    for i in range(num_readers):
        try:
            result = manager.read_data(i, 100)
            results.append(result)
        except Exception as e:
            logger.error(f"查询失败: {e}")

    total_time = time.time() - start_time
    read_results = [r for r in results if r['operation'] == 'SELECT']
    write_results = [r for r in results if r['operation'] == 'INSERT']

    logger.info(f"\n✓ 单线程读写完成:")
    logger.info(f"  - 读操作: {len(read_results)} 次")
    logger.info(f"  - 写操作: {len(write_results)} 次")
    logger.info(f"  - 总耗时: {total_time:.3f}秒")

    return {
        'total_time': total_time,
        'read_count': len(read_results),
        'write_count': len(write_results),
        'total_operations': len(results)
    }


def print_comparison(test_name: str, concurrent_result: dict, single_result: dict):
    """打印性能对比"""
    speedup = single_result['total_time'] / concurrent_result['total_time']

    logger.info(f"\n{'='*60}")
    logger.info(f"性能对比 - {test_name}")
    logger.info(f"{'='*60}")
    logger.info(f"并发执行时间: {concurrent_result['total_time']:.3f}秒")
    logger.info(f"单线程执行时间: {single_result['total_time']:.3f}秒")
    logger.info(f"性能提升: {speedup:.2f}x")
    logger.info(f"时间节省: {(1 - concurrent_result['total_time']/single_result['total_time'])*100:.1f}%")


def main():
    """主函数"""
    logger.info("="*60)
    logger.info("DuckDB 并发 vs 单线程性能对比测试")
    logger.info("="*60)

    # 初始化管理器
    manager = ConcurrentDemoManager("test_performance.duckdb")

    try:
        # 创建测试表
        manager.create_demo_table()

        # ============ 测试1: 写入性能对比 ============
        num_write_threads = 5
        batch_size = 200

        # 单线程写入
        single_write_result = test_single_thread_write(manager, num_write_threads, batch_size)

        # 并发写入
        concurrent_write_result = test_concurrent_write(manager, num_write_threads, batch_size)

        # 打印性能对比
        print_comparison("写入操作", concurrent_write_result, single_write_result)

        # ============ 测试2: 读取性能对比 ============
        num_read_threads = 10
        read_limit = 100

        # 单线程读取
        single_read_result = test_single_thread_read(manager, num_read_threads, read_limit)

        # 并发读取
        concurrent_read_result = test_concurrent_read(manager, num_read_threads, read_limit)

        # 打印性能对比
        print_comparison("读取操作", concurrent_read_result, single_read_result)

        # ============ 测试3: 读写混合性能对比 ============
        num_readers = 8
        num_writers = 2

        # 单线程读写
        single_rw_result = test_single_thread_read_write(manager, num_readers, num_writers)

        # 并发读写
        concurrent_rw_result = test_concurrent_read_write(manager, num_readers, num_writers)

        # 打印性能对比
        print_comparison("读写混合", concurrent_rw_result, single_rw_result)

        # 显示统计信息
        logger.info(f"\n{'='*60}")
        logger.info("数据库统计信息")
        logger.info(f"{'='*60}")
        stats = manager.get_table_stats()
        logger.info(f"总记录数: {stats[0]}")
        logger.info(f"涉及线程数: {stats[1]}")
        logger.info(f"首条记录时间: {stats[2]}")
        logger.info(f"末条记录时间: {stats[3]}")
        logger.info(f"平均值: {stats[4]:.2f}")
        logger.info(f"最小值: {stats[5]:.2f}")
        logger.info(f"最大值: {stats[6]:.2f}")

        # ============ 性能总结 ============
        logger.info(f"\n{'='*60}")
        logger.info("性能总结")
        logger.info(f"{'='*60}")
        logger.info("DuckDB 的 MVCC 并发控制优势：")
        logger.info("  ✓ 读操作完全无锁，可真正并行")
        logger.info("  ✓ 写操作内部串行化，保证数据一致性")
        logger.info("  ✓ 读写并发，互不阻塞")
        logger.info("  ✓ 使用 thread-local cursor，无需应用层加锁")
        logger.info(f"\n性能提升倍数:")
        logger.info(f"  - 写入操作: {single_write_result['total_time']/concurrent_write_result['total_time']:.2f}x")
        logger.info(f"  - 读取操作: {single_read_result['total_time']/concurrent_read_result['total_time']:.2f}x")
        logger.info(f"  - 读写混合: {single_rw_result['total_time']/concurrent_rw_result['total_time']:.2f}x")

        logger.info(f"\n{'='*60}")
        logger.info("✓ 所有测试完成!")
        logger.info(f"{'='*60}")

    except Exception as e:
        logger.error(f"测试失败: {e}")
        raise
    finally:
        manager.close()


if __name__ == "__main__":
    main()
