# -*- coding:UTF-8 -*-
# @Time    : 2025/3/31 09:45
# @Author  : yangxin
# @Email   : yang1819351140@163.com
# @IDE     : PyCharm
# @File    : distributed_queue.py
# @Project : pythonScript
# @Software: PyCharm
import redis
import json
import time
import random
from threading import Thread


class DistributedTaskQueue:
    def __init__(self, queue_name='crawler_tasks', redis_host='localhost', redis_port=6379):
        self.redis = redis.StrictRedis(host=redis_host, port=redis_port, decode_responses=True)
        self.queue_name = queue_name
        self.processing_queue = f'{queue_name}:processing'

    def add_task(self, task_data):
        task_id = f'task_{int(time.time())}_{random.randint(1000, 9999)}'
        task = {'id': task_id, 'data': task_data, 'created_at': time.time()}
        self.redis.lpush(self.queue_name, json.dumps(task))
        return task_id

    def get_task(self, task_id):
        task_json = self.redis.brpoplpush(self.queue_name, self.processing_queue, timeout=0)
        if task_json:
            return json.loads(task_json)
        return None

    def complete_task(self, task_id):
        self.redis.lrem(self.processing_queue, 0, json.dumps(task_id))

    def retry_failed_tasks(self, max_peocessint_time=3600):
        now = time.time()
        tasks = self.redis.lrange(self.processing_queue, 0, -1)

        for task_json in tasks:
            task = json.loads(task_json)
            if now - task['created_at'] > max_peocessint_time:
                self.redis.lpush(self.queue_name, task_json)
                self.redis.lrem(self.processing_queue, 0, task_json)


def worker(queue, worker_id):
    print(f'worker {worker_id} started')
    while True:
        task = queue.get_task()
        if not task:
            time.sleep(1)
            continue

        print(f'worker {worker_id} processing task: {task["id"]}')
        time.sleep(random.randint(1, 3))
        queue.complete_task(task['id'])
        print(f'worker {worker_id} completed task: {task["id"]}')


if __name__ == '__main__':
    queue = DistributedTaskQueue()

    for i in range(10):
        Thread(target=worker, args=(queue, i), daemon=True).start()

    for i in range(100):
        queue.add_task({'url': f'https://example.com/page/{i}'})
        time.sleep(0.1)

    time.sleep(10)
