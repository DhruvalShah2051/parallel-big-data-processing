"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
from mpi4py import MPI
import pandas as pd
import time


class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """
    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self) -> tuple[dict[str:float], list, list, float]:
        """
        Returns a tuple of (final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken).
        """
        comm = MPI.COMM_WORLD
        rank = comm.Get_rank()
        size = comm.Get_size()

        start_time = time.time()

        if rank == 0:
            # Master process reads the dataset and splits it
            data_chunks = list(self.read_dataset_in_chunks(self.dataset_path, size))
            chunk_sizes = [len(chunk) for chunk in data_chunks]

            # Send chunks to worker processes
            for i in range(1, size):
                comm.send(data_chunks[i], dest=i, tag=i)
            
            # Process its own chunk (rank 0)
            top_10_books_per_thread = [self.process_chunk(data_chunks[0])]

            # Receive results from worker processes
            for i in range(1, size):
                top_10_books_per_thread.append(comm.recv(source=i, tag=i))
            
            # Flatten the list of dictionaries into a single dictionary
            flattened_data = {key: value for d in top_10_books_per_thread for key, value in d.items()}
            # Convert the flattened dictionary into a list of tuples
            formatted_data = [(key, value) for key, value in flattened_data.items()]
            # Create a DataFrame from the list of tuples
            df = pd.DataFrame(formatted_data, columns=['BTitle', 'BPrice'])

            df = df.drop_duplicates(subset='BTitle')
            df = df.sort_values(by='BPrice', ascending=False)
            top_10_books = df.head(10).set_index('BTitle')['BPrice'].to_dict()

            total_time_taken = time.time() - start_time

            # Return the final result for the master process
            return top_10_books, chunk_sizes, top_10_books_per_thread, total_time_taken

        else:
            # Worker processes receive their chunk
            data_chunk = comm.recv(source=0, tag=rank)
            name, count, = self.process_chunk(data_chunk)

            # Send result back to the master process
            comm.send((name, count), dest=0, tag=rank)

            # Return dummy values to avoid TypeError in main function
            return 0, [], [], 0.0  # Dummy values for non-master processes

    @staticmethod
    def read_dataset_in_chunks(dataset_path, number_of_chunks):
        df = pd.read_csv(dataset_path)
        chunk_size = len(df) // number_of_chunks
        remainder = len(df) % number_of_chunks

        chunks = []
        for i in range(number_of_chunks):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            if i == number_of_chunks - 1:
                end += remainder
            chunks.append(df.iloc[start:end])
        return chunks

    @staticmethod
    def process_chunk(data_chunk):
        # Convert chunk back to DataFrame
        df_chunk = pd.DataFrame(data_chunk)

        # Calculate average RScore per book
        avg_scores = df_chunk.merge(
            df_chunk.groupby('BId')['RScore'].mean().rename('MeanRScore'),
            on='BId'
        )

        # Filter books with an average RScore < 3
        low_RScores = avg_scores[avg_scores["MeanRScore"] < 3]

        # Get unique books and sort by price in descending order
        unique_books = low_RScores.drop_duplicates(subset='BId')
        sorted_books = unique_books.sort_values(by='BPrice', ascending=False)

        # Get the top 10 books with titles and prices as a dictionary
        top_10_books = sorted_books.head(10).set_index('BTitle')['BPrice'].to_dict()

        return top_10_books

    @staticmethod
    def consolidate_results(all_results):
        # Flatten and consolidate all dictionaries into a single dictionary
        flattened_data = {key: value for d in all_results for key, value in d.items()}

        # Convert to a DataFrame and sort by price in descending order
        df = pd.DataFrame(flattened_data.items(), columns=['BTitle', 'BPrice'])
        df = df.drop_duplicates(subset='BTitle').sort_values(by='BPrice', ascending=False)

        # Extract the top 10 books as a dictionary
        return df.head(10).set_index('BTitle')['BPrice'].to_dict()


if __name__ == '__main__':
    solution = MPISolution(dataset_path="Books_rating.csv", dataset_size=3000000)
    final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken = solution.run()

    if MPI.COMM_WORLD.Get_rank() == 0:
        print(
            {
                "final_answer": final_answer,
                "chunkSizePerThread": chunkSizePerThread,
                "answerPerThread": answerPerThread,
                "totalTimeTaken": totalTimeTaken,
            }
        )
