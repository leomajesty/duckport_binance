import contextlib
import time
from utils.log_kit import logger
import functools
from typing import Callable, Any


@contextlib.contextmanager
def timer(msg=None, log_func=logger.debug):
    begin_time = time.perf_counter()
    yield
    time_elapsed = time.perf_counter() - begin_time
    log_func(f"{msg or 'timer'} | {time_elapsed:.2f} sec elapsed ")

def func_timer(func: Callable) -> Callable:
    """
    函数执行时间装饰器
    
    Args:
        func: 要装饰的函数
        
    Returns:
        装饰后的函数
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        logger.info(f"start func: {func.__name__}")
        
        try:
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.ok(f"func {func.__name__} | {execution_time:.2f} sec elapsed ")
            return result
            
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            logger.error(f"func {func.__name__} | {execution_time:.2f} sec elapsed | error: {e}")
            raise
    
    return wrapper


class Timer:
    """
    上下文管理器计时器
    """
    
    def __init__(self, name: str = "操作"):
        self.name = name
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        print(f"开始 {self.name}...")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        execution_time = self.end_time - self.start_time
        
        if exc_type is None:
            print(f"{self.name} 完成，耗时: {execution_time:.2f}秒")
        else:
            print(f"{self.name} 失败，耗时: {execution_time:.2f}秒，错误: {exc_val}")


def time_function(func: Callable) -> Callable:
    """
    另一种计时装饰器，更简洁的版本
    
    Args:
        func: 要装饰的函数
        
    Returns:
        装饰后的函数
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        
        print(f"{func.__name__}: {end_time - start_time:.4f}秒")
        return result
    
    return wrapper