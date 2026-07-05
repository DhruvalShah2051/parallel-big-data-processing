"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import threading
import time
import pandas as pd
import queue


class ThreadingSolution:
    """
    You are allowed to implement as many methods as you wish
    """

    def __init__(self, num_of_threads=None, dataset_path=None, dataset_size=None):
        self.num_of_threads = num_of_threads
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self) -> tuple[int, list, list, float]:
        """
        Returns the tuple of computed result and time taken. e.g., ("I am final Result", 3.455)
        """
        start_time = time.time()
        chunk_generator = self.read_dataset_in_chunks(self.dataset_path, self.num_of_threads)
        threads = []
        result_queue = queue.Queue()
        for _ in range(self.num_of_threads):
            thread = threading.Thread(target=self.process_chunk, args=(next(chunk_generator), result_queue))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        total_count = 0
        chunk_sizes = []
        count_per_thread = []
        while not result_queue.empty():
            count, data_chunk = result_queue.get()
            total_count += count
            chunk_sizes.append(data_chunk)
            count_per_thread.append(count)

        total_time_taken = time.time() - start_time

        return total_count, chunk_sizes, count_per_thread, total_time_taken
        # raise NotImplementedError("Implement your logic here")

    @staticmethod
    def read_dataset_in_chunks(dataset_path, number_of_threads):
        df = pd.read_csv(dataset_path)
        chunk_size = len(df) // number_of_threads
        remainder = len(df) % number_of_threads

        for i in range(number_of_threads):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            if i == number_of_threads - 1:
                end += remainder
            yield df.iloc[start:end]

    @staticmethod
    def process_chunk(data_chunk, result_queue):
        # calculate average RScore per book
        average_scores = data_chunk.merge(data_chunk.groupby('BId')['RScore'].mean().rename('MeanRScore'), on='BId')
        # filter books with average RScore = 5
        high_RScores = average_scores[average_scores["MeanRScore"] == 5]
        # filter books with price = 1
        price_1 = high_RScores[high_RScores["BPrice"] == 1]
        # find unique books
        count = len(price_1["BId"].unique())
        result_queue.put((count, len(data_chunk)))


if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken = solution.run()
    print(final_answer, totalTimeTaken)
