import subprocess
import uuid
import logging
import inspect
import textwrap
import warnings
from typing import Callable, Optional


class SlurmMultiNodePool:
    def __init__(self,
                 num_tasks: int,
                 job_name: str,
                 log_directory: str,
                 time_limit: str,
                 mem_limit: str,
                 email: str,
                 partition: str = 'owners',
                 python_script_name: Optional[str] = None,
                 slurm_script_name: Optional[str] = None,
                 cpus_per_task: int = 1,
                 ):
        """
        :param num_tasks: the number of tasks to be run on the cluster
        :param job_name: the name of the job
        :param log_directory: the directory to which log files will be written
        :param time_limit: the time limit for the job (be aware this needs to be SLURM compatible) - e.g. 00:10:00
        :param mem_limit: the memory limit for the job (be aware this needs to be SLURM compatible) - e.g. 1G
        :param email: the email address to which notifications will be sent
        :param partition: the partition to which the job will be submitted - e.g. normal, owners,...
        :param python_script_name: the name of the python script that will be created. If None, a random name will be
            generated.
        """

        self.num_tasks = num_tasks
        self.job_name = job_name
        self.log_dir = log_directory
        self.time_limit = time_limit
        self.mem_limit = mem_limit
        self.email = email
        self.partition = partition
        uid = uuid.uuid4()
        if slurm_script_name is None:
            slurm_script_name = f"{job_name}_{uid}.slurm"
        self.slurm_script_name = slurm_script_name
        if python_script_name is None:
            python_script_name = f"{job_name}_{uid}.py"
        self.python_script_name = python_script_name
        self.cpus_per_task = cpus_per_task

        self._task_function = None
        self._task2args_map = None
        self._kwargs = None  # Initialize an attribute to store kwargs

    def create_python_script(self) -> None:
        """
        Creates a python script that will be run on the cluster, saves the file as specified in the constructor.

        :return: None
        """
        if not self._task_function:
            raise ValueError("Task function not set. Use the set_task_function method to set it.")
        if not self._task2args_map:
            raise ValueError("Arguments mapping not set. Please ensure you run _create_task2args_mapping before "
                             "creating the script.")

        # Convert the functions to a string of Python code
        task_func_code = textwrap.dedent(inspect.getsource(self._task_function))
        wrapper_func_code = textwrap.dedent("""
def wrapper(func, args_list, kwargs):
    for arg in args_list:
        if kwargs:
            func(arg, **kwargs)
        else:
            func(arg)
""")
        # Generate the code to include kwargs
        kwargs_code = f"kwargs = {self._kwargs}"

        # Make sure to include wrapper function and any imports it needs
        python_code = f"""
import sys
import logging

{task_func_code}

{wrapper_func_code}

if __name__ == '__main__':
    task_id = int(sys.argv[1])
    {kwargs_code}
    mapping = {self._task2args_map}
    args_list = mapping.get(task_id, [])
    if not args_list:
        raise ValueError(f"No arguments found for task_id {{task_id}}.")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    logging.info(f"Task {{task_id}}: Processing arguments {{args_list}} with kwargs {{kwargs}}")

    # Here, we're calling the wrapper function, passing in the task function, arguments, and kwargs for this task
    wrapper({self._task_function.__name__}, args_list, kwargs)
    logging.info(f"Task {{task_id}}: finished successfully.")
"""
        with open(self.python_script_name, 'w') as script_file:
            script_file.write(python_code)
        logging.info(f"Python script {self.python_script_name} created.")

    def create_slurm_script(self) -> None:
        """
        Creates a SLURM script that will be run on the cluster, saves the file as job name specified in the constructor.

        :return: None
        """
        array_upper_limit = self.num_tasks - 1
        slurm_commands = textwrap.dedent(f"""\
        #!/bin/bash

        ## add additional SBATCH directives here
        #SBATCH --partition={self.partition}
        #SBATCH --job-name={self.job_name}
        #SBATCH --output={self.log_dir}/%j.log
        #SBATCH --time={self.time_limit}
        #SBATCH --mem={self.mem_limit}
        #SBATCH --cpus-per-task={self.cpus_per_task}
        #SBATCH --mail-type=FAIL
        #SBATCH --mail-user={self.email}
        #SBATCH --array=0-{array_upper_limit}

        # load modules, adjust to your needs
        ml devel
        ml python/3.9.0

        # execute the Python code, this should stay as is.
        python3 {self.python_script_name} ${{SLURM_ARRAY_TASK_ID}}
        """)
        # The rest of your code to write slurm_commands to a file would go here

        with open(self.slurm_script_name, 'w') as script_file:
            script_file.write(slurm_commands)
        logging.info(f"SLURM script {self.slurm_script_name} created.")

    def submit_job(self) -> None:
        """
        Submits the job to the cluster. This will only work if the job as already been created before, so if you
        manually call this method, make sure to call create_job first (this is only here so that one can first inspect
        the actual job bash and python files, if necessary.

        :return: None
        """
        subprocess.run(['sbatch', self.slurm_script_name], check=True)
        logging.info(f"Job {self.job_name} submitted.")

    def create_job(self, task_function: Callable, *args, **kwargs) -> None:
        """
        Creates a job on the cluster. This will create a python script and a SLURM script, but will not submit the job.
        See create_submit_job for a more convenient method and full docstring.

        :return: None
        """
        self._task_function = task_function
        self._kwargs = kwargs  # Store kwargs to use in the script
        self._create_task2args_mapping(*args)
        self.create_python_script()
        self.create_slurm_script()

    def create_submit_job(self, task_function: Callable, *args, **kwargs) -> None:
        """
        Creates a job on the cluster. This will create a python script and a SLURM script, and submit the job to the
        cluster. The idea here is a bit similar to Python's multiprocessing module, where you can pass a function and
        arguments to a pool of workers that will execute the function with the arguments. Here, we create a pool of
        workers on the cluster, and each worker will execute the function with one or multiple arguments.

        :param task_function: the function that will be executed once for each argument in args. The function should
            take one argument, and any additional keyword arguments - NOTE that each call will receive the SAME keyword
            arguments, this is more to serve as a context variable. The function must be self-contained, i.e. it cannot
            rely on outside variables. If you need outside variables or dependencies that you cannot pass as arguments,
            you would have to import them within the function:

            Example:
                def my_task_function(arg, **kwargs):
                    from my_fancy_package.my_cool_module import my_cool_function
                    my_cool_function(arg, **kwargs)

        :param args: arguments to be passed to the task function, will be evenly distributed to the cluster based on
            the number of tasks specified. Please note here that args must be primitive data types and cannot rely on
            outside dependencies for the moment.

        :param kwargs: keyword arguments to be passed to the task function, will be passed to each call of the task
            function. Please note here that kwargs must be primitive data types and cannot rely on outside
            dependencies.

        :return: None
        """
        self.create_job(task_function, *args, **kwargs)
        self.submit_job()

    def map(self, task_function: Callable, *args, **kwargs) -> None:
        """
        Same as create_submit_job for people more familiar with the interface of Python's multiprocessing.Pool class.
        """
        self.create_submit_job(task_function, *args, **kwargs)

    def _create_task2args_mapping(self, *args):
        mapping = {}
        if len(args) < self.num_tasks:
            warnings.warn(
                f"Number of arguments ({len(args)}) should be at least the number of tasks ({self.num_tasks})."
                f" To avoid spawning empty tasks, the number of tasks will automatically be reduced to the "
                f"number of arguments.")
            self.num_tasks = len(args)
        # evenly distribute the arguments to the tasks
        for i in range(self.num_tasks):
            mapping[i] = args[i::self.num_tasks]
        self._task2args_map = mapping

