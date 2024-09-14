from worker import Worker

if __name__ == "__main__":
    worker = Worker(name='clean_up-worker', routine_type='Intermittent')
    worker.run()
