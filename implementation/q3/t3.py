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

    def run(self) -> tuple[str, list, list, float]:
        """
        Returns the tuple of (final_answer, chunkSizePerThread, answerPerThread, totalTimeTaken).
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
            name_0, count_0 = self.process_chunk(data_chunks[0])

            max_name = name_0
            max_count = count_0

            name_per_thread = [name_0]
            # Receive results from worker processes
            for i in range(1, size):
                name, count = comm.recv(source=i, tag=i)
                if count > max_count:
                    max_count = count
                    max_name = name
                name_per_thread.append(max_name)
            
            total_time_taken = time.time() - start_time

            # Return the final result for the master process
            return max_name, chunk_sizes, name_per_thread, total_time_taken

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

        # Calculate average score per user
        avg_scores = df_chunk.merge(
            df_chunk.groupby('BId')['RScore'].mean().rename('MeanRScore'),
            on='BId'
        )

        # Filter users with average RScore = 5
        RScore_5 = avg_scores[avg_scores["MeanRScore"] == 5]

        # Calculate the number of reviews per user
        top_users = RScore_5.groupby("UId").size()

        # Find the user with the highest number of reviews
        highest_count_id = top_users.idxmax()
        highest_count_name = df_chunk[df_chunk['UId'] == highest_count_id]["UName"].iloc[0]
        highest_count = top_users.max()

        return highest_count_name, highest_count


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
