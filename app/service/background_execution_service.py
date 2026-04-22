from concurrent.futures import ThreadPoolExecutor


class BackgroundExecutionService:
    def __init__(self):
       self.thread_pool = ThreadPoolExecutor(max_workers=1)

    def submit(self, func, *args, **kwargs):
        self.thread_pool.submit(func, *args, **kwargs)


background_execution = BackgroundExecutionService()
