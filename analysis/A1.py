import sys
import os
import ast

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


def A1()->dict[int,float]:
    """
    Returns the dictionary of (number of workers,TimeTaken)
    """
    total_workers = range(1,11)
    results = []
    for _ in range(3):
        result = []
        for num_of_workers in total_workers:
            _, _, _, totaltimeTaken = run_mpi(num_of_workers,"implementation/q2/t3.py")
            result.append(totaltimeTaken)
        results.append(result)

    averages = [sum(elements) / len(elements) for elements in zip(*results)]
    answer = {i + 1: avg for i, avg in enumerate(averages)}
    return answer
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
