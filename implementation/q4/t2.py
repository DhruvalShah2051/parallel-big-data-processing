"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
import time
import pandas as pd
import multiprocessing


class MultiProcessingSolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, num_of_processes=None, dataset_path=None, dataset_size=None):
        self.num_of_processes = num_of_processes
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self) -> tuple[dict[str:float], list, list, float]:
        """
        Returns the tuple of (final_answer,chunkSizePerThread,answerPerThread,totalTimeTaken)
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

        top_10_books_per_process = []
        chunk_sizes = []
        while not result_queue.empty():
            top_10_books_from_this_process, data_chunk = result_queue.get()
            top_10_books_per_process.append(top_10_books_from_this_process)
            chunk_sizes.append(data_chunk)

        # Flatten the list of dictionaries into a single dictionary
        flattened_data = {key: value for d in top_10_books_per_process for key, value in d.items()}
        # Convert the flattened dictionary into a list of tuples
        formatted_data = [(key, value) for key, value in flattened_data.items()]
        # Create a DataFrame from the list of tuples
        df = pd.DataFrame(formatted_data, columns=['BTitle', 'BPrice'])

        df = df.drop_duplicates(subset='BTitle')
        df = df.sort_values(by='BPrice', ascending=False)
        top_10_books = df.head(10).set_index('BTitle')['BPrice'].to_dict()

        total_time_taken = time.time() - start_time

        return top_10_books, chunk_sizes, top_10_books_per_process, total_time_taken
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
        # calculate average RScore per book
        average_scores = data_chunk.merge(data_chunk.groupby('BId')['RScore'].mean().rename('MeanRScore'), on='BId')
        # filter books with an average RScore < 3
        low_RScores = average_scores[average_scores["MeanRScore"] < 3]
        # find unique books
        unique_books = low_RScores.drop_duplicates(subset='BId')
        # sort the books based on price in descending order
        sorted_books = unique_books.sort_values(by='BPrice', ascending=False)
        # find the top 10 books and return the title and price as a dictionary
        top_10_books = sorted_books.head(10).set_index('BTitle')['BPrice'].to_dict()

        result_queue.put((top_10_books, len(data_chunk)))


if __name__ == '__main__':
    solution = MultiProcessingSolution(num_of_processes=4, dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken = solution.run()
    print(final_answer, totalTimeTaken)
