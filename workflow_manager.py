from __future__ import annotations
import fcntl
import inspect
import json
import logging
import os
import select
import shlex
import subprocess
import sys
from abc import abstractmethod, abstractclassmethod
from pathlib import Path
from typing import Iterable, Any, List, Sequence, Optional


logger = logging.getLogger(__name__)

# ToDo: Instead of explicitly defining pre-req jobs, let them be defined naturally from the parameter map? -- Won't do
# ToDo: Have the job result inherit the job and automatically pass the fixed parameters and mappings into the results
# ToDo: Include job_id in JobResults

def asdict(*vars):
    frame = inspect.currentframe().f_back
    result = {}
    for v in vars:
        for name, val in frame.f_locals.items():
            if v is val:
                result[name] = v
                break
    return result


class StringyJSONEncoder(json.encoder.JSONEncoder):
    """
    Falls back to converting an object to string (.__str__) if it can't be directly encoded. 
    """
    def default(self, o):
        try:
            return str(o)
        except TypeError:
            return super().default(o)

            
class Environment:
    def __init__(self, conda_command: str='', conda_env: str='bio', polling_interval: float=0.1):
        self.conda_command = conda_command
        self.conda_env = conda_env
        self.polling_interval = polling_interval
        
    def run_parallel(self, cmds: List[str], use_conda: bool=False):
        procs = []
        for cmd in cmds:
            cmd = self.prepare_command(cmd, use_conda=use_conda)
            logger.debug(f"Launching: {cmd}")
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            procs.append(p)
    
        # Wait for completion and propagate errors
        outputs = []
        for p in procs:
            return_code = p.wait()
            if return_code != 0:
                raise RuntimeError(f"Command {cmds[procs.index(p)]} failed (return_code={return_code}): {p.stderr.read()}.")
            else:
                stdout = p.stdout.read()
                stderr = p.stderr.read()
                outputs.append({'return_code': return_code, 'stdout': stdout, 'stderr': stderr})

        return outputs
        
    def prepare_command(self, cmd: str, use_conda: bool=False):
        base_cmd = ['bash', '-c', f'set -euo pipefail; {cmd}']
        if self.conda_command and use_conda:
            return [self.conda_command, 'run', '-n', self.conda_env] + base_cmd
        else:
            return base_cmd
            
    def prepare_command_streaming(self, cmd: str, use_conda: bool=False):
        # Uses stdbuf to coerce line buffering
        base_cmd = ['stdbuf', '-oL', '-eL', 'bash', '-c', f'set -euo pipefail; {cmd}']
        if self.conda_command and use_conda:
            return [self.conda_command, 'run', '-n', self.conda_env] + base_cmd
        else:
            return base_cmd

    @staticmethod
    def _set_nonblocking(f):
        fd = f.fileno()
        fl = fcntl.fcntl(fd, fcntl.F_GETFL)
        fcntl.fcntl(fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        
    def run_parallel_streaming(self, cmds: List[str], use_conda: bool=False):
        """Run multiple shell commands in parallel, streaming logs in real time."""
        procs = []
        outputs = []
        buffers = {}  # partial line buffers per (proc_idx, stream)
    
        # Launch processes
        for idx, raw in enumerate(cmds):
            cmd = self.prepare_command_streaming(raw, use_conda=use_conda)
            logger.debug(f'Launching CMD {idx}: {cmd}')
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                close_fds=True,
            )
            self._set_nonblocking(p.stdout)
            self._set_nonblocking(p.stderr)
            procs.append(p)
            outputs.append({"cmd": cmd, "stdout": "", "stderr": "", "return_code": None})

        live = set(range(len(procs)))
    
        while live:
            # rebuild fd map
            fd_map = {}
            read_fds = []
            for i in list(live):
                for which, stream in (("stdout", procs[i].stdout), ("stderr", procs[i].stderr)):
                    if stream is None:
                        continue
                    fd = stream.fileno()
                    fd_map[fd] = (i, which)
                    read_fds.append(fd)
    
            if not read_fds:
                # no fds left, just poll for exit
                for i in list(live):
                    rc = procs[i].poll()
                    if rc is not None:
                        outputs[i]["return_code"] = rc
                        live.remove(i)
                        if rc != 0:
                            raise RuntimeError(f"CMD {i} failed (rc={rc}): {cmds[i]}")
                sleep(self.polling_interval)
                continue
    
            # Wait for data
            rready, _, _ = select.select(read_fds, [], [], self.polling_interval)
    
            for fd in rready:
                idx, which = fd_map[fd]
                p = procs[idx]
                stream = p.stdout if which == "stdout" else p.stderr
                try:
                    chunk = stream.read()
                except BlockingIOError:
                    chunk = None
    
                if not chunk:
                    continue
    
                bufkey = (idx, which)
                prev = buffers.get(bufkey, "")
                combined = prev + chunk
    
                # Split by lines but keep last partial line in buffer
                lines = combined.splitlines(keepends=True)
                if not combined.endswith(("\n", "\r")):
                    # last line incomplete
                    buffers[bufkey] = lines[-1] if lines else combined
                    lines = lines[:-1]
                else:
                    buffers[bufkey] = ""
    
                for line in lines:
                    text = line.rstrip("\r\n")
                    logger.info(f"[CMD {idx} {which}]: {text}")
                    outputs[idx][which] += line
    
            # check process exits
            for i in list(live):
                rc = procs[i].poll()
                if rc is not None:
                    # flush any buffered remainder
                    for which in ("stdout", "stderr"):
                        tail = buffers.pop((i, which), "")
                        if tail:
                            logger.info(f"[CMD {i} {which}]: {tail}")
                            outputs[i][which] += tail
                    outputs[i]["return_code"] = rc
                    live.remove(i)
                    if rc != 0:
                        msg = f"CMD {i} failed (rc={rc}): {cmds[i]}"
                        logger.error(msg)
                        raise RuntimeError(msg)

        return outputs

    def run(self, cmd: str, check=True, use_conda: bool=False):
        return self.run_parallel([cmd], use_conda=use_conda)[0]
        
    def run_streaming(self, cmd: str, check=True, use_conda: bool=False):
        return self.run_parallel_streaming([cmd], use_conda=use_conda)[0]
        
    def capture(self, cmd: str, use_conda: bool=False):
        output = self.run(cmd, use_conda=use_conda)
        return output['stdout']

    @staticmethod
    def ensure_dir(p):
        logger.debug(f'Creating path {str(p)} if needed ...')
        Path(p).mkdir(parents=True, exist_ok=True)

        
class Job:
    def __init__(self, job_id: str, 
                 prereq_job_ids: Iterable=[], 
                 fixed_params: Dict[str, Any]={},
                 prereq_params: Dict[str, str]={},
                 environment: Environment=None):
        self.job_id = job_id
        
        self.prereq_job_ids = set(prereq_job_ids)
        for param_source in prereq_params.values():
            self.prereq_job_ids.add(param_source.split('.')[0])
    
        self.environment = environment
        self.fixed_params = fixed_params
        self.prereq_params = prereq_params

        self.parent_workflow = None
        self.prereq_results = {}

    def get_param(self, param_name: str):
        """Looks first in the fixed params declared at init time, then in the pre-req jobs defined by the pre-req parameter mapping, then in the attributes
        of the parent workflow.
        """
        if param_name in self.fixed_params:
            return self.fixed_params[param_name]
        
        # print(param_name in self.prereq_params)

        elif param_name in self.prereq_params:
            if '.' in self.prereq_params[param_name]:
                source_job, source_key = self.prereq_params[param_name].split('.')
            else:
                source_job = self.prereq_params[param_name]
                source_key = param_name
            return self.prereq_results[source_job][source_key]
        else:
            try:
                return self.parent_workflow.__getattribute__(param_name)
            except AttributeError:
                pass
        raise KeyError(f'Param {param_name} not found in fixed params, prereq results or parent workflow!')

    def __getattribute__(self, name):
        # 1. Always use super() for normal attributes
        try:
            # logger.debug(f"checking superclass for {name}")
            return super().__getattribute__(name)
        except AttributeError:
            pass

        # 2. Fallback to internal structures
        try:
            # logger.debug(f"Calling self.get_param({name})")
            return self.get_param(name)
        except KeyError:
            pass

        # 3. Standard failure semantics
        raise AttributeError(f"{type(self).__name__!r} has no attribute {name!r}")

    def __repr__(self):
        return '\n'.join([f"Job: {self.job_id}",
                          f"Fixed parameters: {self.fixed_params}",
                          f"Pre-req jobs: {','.join(self.prereq_job_ids)}",
                          f"Parameter to pre-req job mappings: {self.prereq_params}"
                         ])
    
    @abstractmethod
    def do_work(self) -> JobResult:
        pass


class JobResult:
    def __init__(self, success: bool, completed: bool=True, **kwargs):
        self.values = {'success': success, 'completed': completed} | kwargs

    @property
    def success(self):
        return self.values['success']

    @property
    def completed(self):
        return self.values['completed']

    @property
    def good(self):
        return self.completed and self.success

    @classmethod
    def load_from_json(cls, result_file_fpath: Path | str):
        try:
            with open(result_file_fpath, 'rt') as result_file:
                logger.debug(f'Loading job result from {result_file_fpath} ...')
                result_json = json.load(result_file)
        except FileNotFoundError:
            logger.debug(f'Job result {result_file_fpath} not found. Assuming job is incomplete.')
            return cls(completed=False, success=False)
        else:
            logger.debug(f'Loaded job result {result_json} from {result_file_fpath}.')
            return cls(**result_json)

    def to_json(self):
        return json.dumps(self.values, cls=StringyJSONEncoder)

    def to_dict(self):
        return self.values.copy()
            
    def write_to_json(self, result_file_fpath: Path | str):
        with open(result_file_fpath, 'wt') as result_file:
            result_json_string = self.to_json()
            logger.debug(f'Writing job result {result_json_string} to {result_file_fpath} ...')
            result_file.write(result_json_string)
            
    def __repr__(self):
        return '\n'.join([f"JobResult - Completed: {self.completed}, success: {self.success}, values: {self.values}",
                         ])

    def __getitem__(self, key):
        return self.values[key]


class SuccessResult(JobResult):
    def __init__(self, **kwargs):
        super().__init__(success=True, completed=True, **kwargs)


class FailureResult(JobResult):
    def __init__(self, **kwargs):
        super().__init__(success=False, completed=True, **kwargs)

        
class Workflow:
    result_fname_template = '{workflow_id}-{sample_id}-{job_id}.json'
    
    def __init__(self, workflow_id: str, 
                 sample_id: str,
                 result_path: Path | str, 
                 job_list: Iterable[Job], 
                 environment: Environment,
                 retry_failed: bool=True,
                 abort_on_error: bool=True):
        self.workflow_id = workflow_id
        self.sample_id = sample_id
        self.result_path = Path(result_path)
        self.environment = environment
        self.retry_failed = retry_failed
        self.abort_on_error = abort_on_error
        
        self.job_list = job_list
        for job in self.job_list:
            job.parent_workflow = self
            if job.environment is None:
                job.environment = self.environment
            
        self.job_dict = {job.job_id: job for job in self.job_list}
        self.environment.ensure_dir(result_path)

    def generate_result_file_fpath(self, job_id):
        return self.result_path.joinpath(self.result_fname_template.format(workflow_id=self.workflow_id,
                                                                                   sample_id=self.sample_id,
                                                                                   job_id=job_id))

    def get_result(self, job_id):
        result_file_fpath = self.generate_result_file_fpath(job_id=job_id)
        result = JobResult.load_from_json(result_file_fpath)
        logger.debug(f'get_result for {result_file_fpath}, values: {result.values}')
        return result

    def write_result(self, job_id: str, result: JobResult):
        result_file_fpath = self.generate_result_file_fpath(job_id=job_id)
        result.write_to_json(result_file_fpath)
            
    def execute_job(self, job_id: str):
        """
        Attempts to return the result for :param:`job_id`. If needed, will execute the job and
        any necessary pre-requisite jobs.
        
        Returns either the JobResult for the current job or the JobResult for the most upstream blocking failure.
        """
        # ToDo: Clean up logic and flow control!        
        logger.info(f'Executing job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} ...')
        this_job = self.job_dict[job_id]
        result = self.get_result(job_id)           
        
        run_job = False
        if result.completed is True:
            if result.success is True:
                logger.info(f'Job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} already completed successfully with result: {result}.')
                return result
            else:
                logger.info(f'Job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} previously failed with result: {result}!')
                if self.retry_failed:
                    run_job = True
                    logger.info(f'Will retry job {job_id} on sample {self.sample_id} in workflow {self.workflow_id}.')
                else:
                    return result
        else:
            logger.info(f'Job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} is not complete. Will try to complete.')
            run_job = True

        if run_job:
            prereqs = this_job.prereq_job_ids
            logger.info(f'Checking prereq jobs: {prereqs}')
            prereq_results = {job_id:self.execute_job(job_id) for job_id in prereqs}
            for prereq_job_id, prereq_result in prereq_results.items():
                if not prereq_result.completed:
                    logger.error(f'Pre-req {prereq_job_id} for job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} did not complete.')                    
                    return prereq_result                    
                if not prereq_result.success:
                    logger.error(f'Pre-req {prereq_job_id} for job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} failed with results: {prereq_result.values}.')
                    return prereq_result

            this_job.prereq_results = prereq_results
            logger.info(f'Doing work for job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} ...')
            this_job_result = this_job.do_work()
            logger.info(f'Work for job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} got result: {this_job_result}.')
        
            if not this_job_result.completed:
                logger.error(f'Job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} did not complete.')                    
                return this_job_result                
            if not this_job_result.success:
                logger.error(f'Job {job_id} on sample {self.sample_id} in workflow {self.workflow_id} failed with results: {this_job_result}.')

            self.write_result(job_id=job_id, result=this_job_result)
            return this_job_result
          