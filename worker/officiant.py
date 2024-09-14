from worker import Worker

if __name__ == "__main__":
    worker = Worker(name='officiant-worker', routine_type='Concentrated')
    worker.run()
