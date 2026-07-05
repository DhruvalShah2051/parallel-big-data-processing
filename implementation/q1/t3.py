"""
You are allowed use necessary python libraries.
You are not allowed to have any global function or variables.
"""
from mpi4py import MPI
import time
import pandas as pd


class MPISolution:
    """
    You are allowed to implement as many methods as you wish
    """

    def __init__(self, dataset_path=None, dataset_size=None):
        self.dataset_path = dataset_path
        self.dataset_size = dataset_size

    def run(self) -> tuple[int, list, list, float]:
        """
        Returns the tuple of computed result and time taken. e.g., ("I am final Result", 3.455)
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
            count_0 = self.process_chunk(data_chunks[0])

            # Receive results from worker processes
            total_count = count_0
            counts_per_thread = [count_0]
            for i in range(1, size):
                count = comm.recv(source=i, tag=i)
                total_count += count
                counts_per_thread.append(count)

            total_time_taken = time.time() - start_time

            # Return the final result for the master process
            return total_count, chunk_sizes, counts_per_thread, total_time_taken

        else:
            # Worker processes receive their chunk
            data_chunk = comm.recv(source=0, tag=rank)
            count, _ = self.process_chunk(data_chunk)

            # Send result back to the master process
            comm.send(count, dest=0, tag=rank)

            # Return dummy values to avoid TypeError in main function
            return 0, [], [], 0.0  # Dummy values for non-master processes
        # raise NotImplementedError("Implement your logic here")

    @staticmethod
    def read_dataset_in_chunks(dataset_path, number_of_threads):
        df = pd.read_csv(dataset_path)
        chunk_size = len(df) // number_of_threads
        remainder = len(df) % number_of_threads

        chunks = []
        for i in range(number_of_threads):
            start = i * chunk_size
            end = (i + 1) * chunk_size
            if i == number_of_threads - 1:
                end += remainder
            chunks.append(df.iloc[start:end])
        return chunks

    @staticmethod
    def process_chunk(data_chunk):
        df = pd.DataFrame(data_chunk)
        count = sum(score < 4 for score in df['RScore'])
        return count


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
