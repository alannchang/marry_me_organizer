from worker import Worker

if __name__ == "__main__":
    worker = Worker(name='waiters-worker', routine_type='Standard')
    worker.run()
