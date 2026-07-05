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

    def run(self) -> tuple[str, list, list, float]:
        """
        Returns the tuple of (final_answer,chunkSizePerThread,answerPerThread,totalTimeTaken)
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

        results = {}
        chunk_sizes = []
        name_per_thread = []
        while not result_queue.empty():
            (name, count), data_chunk = result_queue.get()
            if name in results:
                results[name] += count
            else:
                results[name] = count
            name_per_thread.append(name)
            chunk_sizes.append(data_chunk)
            name_per_thread.append(name)

        max_name = max(results, key=results.get)

        total_time_taken = time.time() - start_time

        return max_name, chunk_sizes, name_per_thread, total_time_taken
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
        # average score per user
        average_scores = data_chunk.merge(data_chunk.groupby('BId')['RScore'].mean().rename('MeanRScore'), on='BId')
        # filter users with average RScore = 5
        RScore_5 = average_scores[average_scores["MeanRScore"] == 5]
        # calculate the number of reviews of each user
        top_users = RScore_5.groupby("UId").size()
        # find the user with the highest number of reviews
        highest_count_id = top_users.idxmax()
        highest_count_name = data_chunk[data_chunk['UId'] == highest_count_id]["UName"].iloc[0]
        highest_count = top_users.max()

        result_queue.put(((highest_count_name, highest_count), len(data_chunk)))


if __name__ == '__main__':
    solution = ThreadingSolution(num_of_threads=4, dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken = solution.run()
    print(final_answer, totalTimeTaken)
