# SlurmMultiNodePool Module

## Overview
The `SlurmMultiNodePool` module is designed for managing and executing tasks on a SLURM-based multi-node cluster. It 
allows users to easily create, submit, and manage jobs that can span multiple nodes in a SLURM cluster.
This package was tested and used on Stanford's Sherlock cluster but should work on most SLURM-based HPC clusters.

## Features
- Simple distributed execution of Python functions on a SLURM cluster
- Manages generation of SLURM scripts for job submission automatically
- Configure job parameters such as memory, time limits, and logging
- Leightweight and easy to use

## Prerequisites
- A SLURM cluster
- Python 3 environment

## Installation
Copy the class `SlurmMultiNodePool` in the `main` module where you want to use it.
OR install the package using pip:
```bash
pip install git+https://github.com/NarayanSchuetz/SlurmMultiNodePool.git
```

and then import it in your code:
```python
from slurm_pool import SlurmMultiNodePool
```

## Usage
1. **Initialize the Job Manager:**
   Create an instance of `SlurmMultiNodePool` with the required parameters like number of tasks, job name, logging directory, time limit, memory limit, and email for notifications.

   Example:
   ```python
   job_manager = SlurmMultiNodePool(num_tasks=5, job_name='test', log_directory='.',
                                    time_limit='00:10:00', mem_limit='1G', email="example@email.com",
                                    partition='normal', python_script_name='tmp.py')

2. **Create and Submit a Job**:
Define a task function that you want to run on the cluster. Use the `create_job` or `create_submit_job` method to create and optionally submit the job to the cluster directly.
    
   Example:
   ```python
   def test_function(arg):
       print("Task:", arg)

   job_manager.create_submit_job(test_function, *["task1", "task2", "task3"])
   # or if you prefer
   job_manager.map(test_function, *["task1", "task2", "task3"])

3. **Logging**:
   Logs are written to the specified directory, providing information about the execution of tasks.

## Customization
You can customize job parameters such as the number of CPUs per task, partition, and the script names as per your cluster configuration and requirements.

## Note
Ensure that the task function and its arguments are self-contained and do not rely on external dependencies that are not available on the cluster nodes.
You may import modules and packages within the task function, but ensure that they are available on the cluster.

## License
This project is licensed under the MIT License.

