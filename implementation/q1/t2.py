"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import time
import multiprocessing
import pandas as pd


class MultiProcessingSolution:
    """
    You are allowed to implement as many methods as you wish
    """

    def __init__(self, num_of_processes=None, dataset_path=None, dataset_size=None):
        self.num_of_processes = num_of_processes
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self) -> tuple[int, list, list, float]:
        """
        Returns the tuple of computed result and time taken. e.g., ("I am final Result", 3.455)
        """
        start_time = time.time()
        chunk_generator = self.read_dataset_in_chunks(self.dataset_path, self.num_of_processes)
        processes = []
        result_queue = multiprocessing.Queue()
        for _ in range(self.num_of_processes):
            process = multiprocessing.Process(target=self.process_chunk, args=(next(chunk_generator), result_queue))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        total_count = 0
        chunk_sizes = []
        count_per_process = []
        while not result_queue.empty():
            count, data_chunk = result_queue.get()
            total_count += count
            chunk_sizes.append(data_chunk)
            count_per_process.append(count)

        total_time_taken = time.time() - start_time
        return total_count, chunk_sizes, count_per_process, total_time_taken
        # raise NotImplementedError("Implement your logic here")

    @staticmethod
    def read_dataset_in_chunks(dataset_path, number_of_processes):
        df = pd.read_csv(dataset_path)
        chunk_size = len(df) // number_of_processes
        remainder = len(df) % number_of_processes

        for i in range(number_of_processes):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            if i == number_of_processes - 1:
                end += remainder
            yield df.iloc[start:end]

    @staticmethod
    def process_chunk(data_chunk, result_queue):
        count = sum(score < 4 for score in data_chunk['RScore'])
        result_queue.put((count, len(data_chunk)))


if __name__ == '__main__':
    solution = MultiProcessingSolution(num_of_processes=4, dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken = solution.run()
    print(final_answer, totalTimeTaken)
