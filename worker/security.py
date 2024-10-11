from worker import Worker

if __name__ == "__main__":
    worker = Worker(name='security-worker', routine_type='Standard')
    worker.run()
