import sys
import os
import ast

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from implementation.q3.t1 import ThreadingSolution as Q3T1
from implementation.q3.t2 import MultiProcessingSolution as Q3T2


def A2()->dict[str,dict[int,float]]:
    """
    Return a dictionary of format {MT:{1:t1,2:t2, ..}, MP:{...}, MPI:{...}} where t is the time in seconds.
    """
    total_workers = range(1,11)


    # record time for multithreading
    multithreading_results = []
    for _ in range(3):
        result = []
        for num_of_workers in total_workers:
            q3t1 = Q3T1(num_of_workers, dataset_path="Books_rating.csv", dataset_size=3000000)
            _, _, _, totalTimeTaken= q3t1.run()
            result.append(totalTimeTaken)
        multithreading_results.append(result)
    
    multithreading_averages = [sum(elements) / len(elements) for elements in zip(*multithreading_results)]
    multithreading_answer = {i + 1: avg for i, avg in enumerate(multithreading_averages)}


    # record time for multiprocessing
    multiprocessing_results = []
    for _ in range(3):
        result = []
        for num_of_workers in total_workers:
            q3t2 = Q3T2(num_of_workers, dataset_path="Books_rating.csv", dataset_size=3000000)
            _, _, _, totalTimeTaken= q3t2.run()
            result.append(totalTimeTaken)
        multiprocessing_results.append(result)
    
    multiprocessing_averages = [sum(elements) / len(elements) for elements in zip(*multiprocessing_results)]
    multiprocessing_answer = {i + 1: avg for i, avg in enumerate(multiprocessing_averages)}


    # record time for MPI
    mpi_results = []
    for _ in range(3):
        result = []
        for num_of_workers in total_workers:
            _, _, _, totalTimeTaken= run_mpi(num_of_workers,"implementation/q3/t3.py")
            result.append(totalTimeTaken)
        mpi_results.append(result)
    
    mpi_averages = [sum(elements) / len(elements) for elements in zip(*mpi_results)]
    mpi_answer = {i + 1: avg for i, avg in enumerate(mpi_averages)}

    final_answer = {
        "MT": multithreading_answer,
        "MP": multiprocessing_answer,
        "MPI": mpi_answer
    }

    return final_answer
    # raise NotImplementedError("Implement your logic here")

def run_mpi(num_workrs,task_path):
    import subprocess
    # Define the command to run the MPI program
    command = ['mpiexec', '-n', str(num_workrs), 'python', task_path]
    try:
        out = subprocess.run(command, text=True, capture_output=True, check=True)    
        # print("out=",out.stdout)
        # print("ast.literal_eval(out)",ast.literal_eval(out.stdout))
        return tuple(ast.literal_eval(out.stdout).values())
    except subprocess.CalledProcessError as e:
        print("An error occurred while running the MPI program.")
        print("Return code:", e.returncode)
        print("Output:\n", e.stdout)
        print("Errors:\n", e.stderr)
