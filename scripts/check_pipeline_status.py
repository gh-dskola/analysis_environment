#!/usr/bin/env python3
"""
Check Nextflow pipeline status and provide structured summary.

Usage:
    check_pipeline_status.py --outdir /path/to/pipeline/output [options]

Outputs YAML summary with:
- Overall pipeline status
- Per-sample progress
- Process-level statistics
- Active SLURM jobs
- Errors and retry reasons
- Cached task information
"""

import argparse
import json
import os
import re
import signal
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

# Handle SIGPIPE gracefully for piping to tools like head, jq, python
# Without this, piping to a command that closes early causes BrokenPipeError
signal.signal(signal.SIGPIPE, signal.SIG_DFL)


@dataclass
class TaskInfo:
    """Information about a single task execution."""
    task_id: str
    process: str
    sample: str
    status: str
    exit_code: Optional[int] = None
    duration: Optional[str] = None
    cached: bool = False
    retry_count: int = 0
    error_message: Optional[str] = None
    work_dir: Optional[str] = None
    native_id: Optional[str] = None


@dataclass
class SampleStatus:
    """Track the status of each sample through the pipeline."""
    sample_id: str
    current_stage: str = "unknown"
    stages_complete: list = field(default_factory=list)
    has_error: bool = False
    error_details: Optional[str] = None
    retry_count: int = 0


@dataclass
class TaskOutcome:
    """Resolved final outcome of a logical task across all its retry attempts."""
    task_name: str
    process: str
    sample: str
    final_status: str  # "COMPLETED", "CACHED", or "FAILED"
    attempts: int
    failed_attempts: int
    last_exit_code: Optional[int] = None
    last_native_id: Optional[str] = None


@dataclass
class PipelineStatus:
    """Overall pipeline status."""
    status: str = "unknown"
    started: Optional[str] = None
    duration: Optional[str] = None
    completed_at: Optional[str] = None
    work_dir: Optional[str] = None
    total_tasks: int = 0
    succeeded: int = 0
    cached: int = 0
    failed: int = 0
    running: int = 0
    pending: int = 0
    retries: int = 0


# Define pipeline stage order for progress tracking
STAGE_ORDER = [
    "RECORD_RUN_METADATA",
    # Alignment/Manta path
    "BUILD_ALIGNMENT_CONTAINER",
    "TRIM_ALIGN_SORT",
    "SPLIT_FASTQ",
    "TRIM_ALIGN_SORT_PART",
    "MERGE_SORTED_BAMS",
    "MARK_DUPLICATES",
    "INDEX_BAM",
    "BUILD_MANTA_CONTAINER",
    "BUILD_MANTA_POST_CONTAINER",
    "MANTA_SOMATIC",
    "POST_PROCESS_MANTA",
    # Fusion/ABFusion path
    "BUILD_FUSION_ALIGNMENT_CONTAINER",
    "FUSION_ALIGN_PART",
    "FUSION_ALIGN_MERGE",
    "BUILD_FUSIONCALLER_BINARIES",
    "BUILD_ABFUSION_CONTAINER",
    "ABFUSION_CALL",
    "ABFUSION_FILTER",
]


def _extract_process_and_sample(name: str) -> tuple[str, str]:
    """Extract process name and normalized sample ID from a task name.

    Args:
        name: Full task name, e.g. "from_fastq:ABFUSION:ABFUSION_CALL (A1093741_tumor)"

    Returns:
        Tuple of (process, sample) where sample is normalized (stripped of
        _tumor/_normal suffixes and _partN suffixes).
    """
    process = ""
    sample = ""
    if name:
        process_match = re.search(r':?([A-Z_]+)\s*\(', name)
        if process_match:
            process = process_match.group(1)

        sample_match = re.search(r'\(([^)]+)\)', name)
        if sample_match:
            sample = sample_match.group(1)
            sample = re.sub(r'_(tumor|normal).*$', '', sample)
            sample = re.sub(r'_part\d+$', '', sample)

    return process, sample


def parse_execution_trace(
    trace_path: Path,
) -> tuple[dict[str, TaskInfo], dict[str, TaskOutcome]]:
    """Parse execution_trace.txt for task information and resolve final outcomes.

    Groups trace entries by task name and determines whether each logical task
    ultimately succeeded (any attempt COMPLETED/CACHED) or permanently failed
    (all attempts FAILED).

    Returns:
        tasks: Individual task entries keyed by hash:process:sample.
        task_outcomes: Final resolved outcomes keyed by full task name,
            grouping all retry attempts of each logical task.
    """
    tasks = {}
    tasks_by_name: dict[str, list[TaskInfo]] = defaultdict(list)

    if not trace_path.exists():
        return tasks, {}

    with open(trace_path) as f:
        header = None
        for line in f:
            line = line.strip()
            if not line:
                continue

            parts = line.split('\t')

            if header is None:
                header = parts
                continue

            if len(parts) < len(header):
                continue

            row = dict(zip(header, parts))

            task_id = row.get('hash', row.get('task_id', ''))
            name = row.get('name', '')
            status = row.get('status', '')
            exit_code = row.get('exit', '')
            duration = row.get('duration', row.get('realtime', ''))
            work_dir = row.get('workdir', '')
            native_id = row.get('native_id', '')

            process, sample = _extract_process_and_sample(name)

            task = TaskInfo(
                task_id=task_id,
                process=process,
                sample=sample,
                status=status,
                exit_code=int(exit_code) if exit_code and exit_code != '-' else None,
                duration=duration if duration and duration != '-' else None,
                cached=status == 'CACHED',
                work_dir=work_dir,
                native_id=native_id if native_id and native_id != '-' else None,
            )

            # Use task_id + process + sample as key to handle retries
            key = f"{task_id}:{process}:{sample}"
            tasks[key] = task
            tasks_by_name[name].append(task)

    # Resolve final outcome for each logical task
    task_outcomes = {}
    for task_name, attempts in tasks_by_name.items():
        if not task_name:
            continue

        any_succeeded = any(
            t.status in ('COMPLETED', 'CACHED') for t in attempts
        )
        failed_attempts = [t for t in attempts if t.status == 'FAILED']
        last_attempt = attempts[-1]  # trace is ordered chronologically

        process, sample = _extract_process_and_sample(task_name)

        if any_succeeded:
            final_status = "COMPLETED"
        else:
            final_status = "FAILED"

        task_outcomes[task_name] = TaskOutcome(
            task_name=task_name,
            process=process,
            sample=sample,
            final_status=final_status,
            attempts=len(attempts),
            failed_attempts=len(failed_attempts),
            last_exit_code=last_attempt.exit_code,
            last_native_id=last_attempt.native_id,
        )

    return tasks, task_outcomes


def parse_nextflow_log(
    log_path: Path,
    task_outcomes: dict[str, TaskOutcome],
) -> tuple[PipelineStatus, list[dict]]:
    """Parse nextflow.log for pipeline status and permanent error details.

    Only reports permanently failed tasks (where the error was ignored after
    exhausting retries or because the exit code wasn't retryable). Transient
    retry events are counted as a summary stat (``pipeline.retries``) but not
    reported as errors.

    Cross-references with *task_outcomes* from the execution trace to confirm
    that the task did not eventually succeed on a later attempt.
    """
    pipeline = PipelineStatus()
    errors = []

    if not log_path.exists():
        return pipeline, errors

    with open(log_path) as f:
        content = f.read()

    # Extract start time
    start_match = re.search(r'(\w{3}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+).*Session.*', content)
    if start_match:
        pipeline.started = start_match.group(1)

    # Extract work directory
    workdir_match = re.search(r"Work Dir\s*:\s*(\S+)", content)
    if workdir_match:
        pipeline.work_dir = workdir_match.group(1)

    # Check if pipeline completed
    if "Workflow completed" in content or "Pipeline completed" in content:
        pipeline.status = "completed"

        # Extract completion time
        complete_match = re.search(r'Completed at\s*:\s*(.+)', content)
        if complete_match:
            pipeline.completed_at = complete_match.group(1).strip()

        # Extract duration
        duration_match = re.search(r'Duration\s*:\s*(.+)', content)
        if duration_match:
            pipeline.duration = duration_match.group(1).strip()
    else:
        pipeline.status = "running"

    # Extract task summary if available
    succeeded_match = re.search(r'Succeeded\s*:\s*(\d+)', content)
    if succeeded_match:
        pipeline.succeeded = int(succeeded_match.group(1))

    cached_match = re.search(r'Cached\s*:\s*(\d+)', content)
    if cached_match:
        pipeline.cached = int(cached_match.group(1))

    failed_match = re.search(r'Failed\s*:\s*(\d+)', content)
    if failed_match:
        pipeline.failed = int(failed_match.group(1))

    # Count retry events as a summary stat (not errors)
    retry_pattern = re.compile(
        r'Process `[^`]+` terminated with an error exit status \(\d+\) -- Execution is retried \(\d+\)'
    )
    pipeline.retries = len(retry_pattern.findall(content))

    # Find permanently failed tasks: "Error is ignored" means all retries
    # exhausted (or exit code wasn't retryable) and the task was abandoned
    ignored_pattern = re.compile(
        r'Process `([^`]+)` terminated with an error exit status \((\d+)\) -- Error is ignored'
    )
    for match in ignored_pattern.finditer(content):
        full_process_name = match.group(1)
        exit_code = int(match.group(2))

        # Extract sample from process name
        sample_match = re.search(r'\(([^)]+)\)', full_process_name)
        sample = sample_match.group(1) if sample_match else "unknown"

        # Extract just the process type
        process_match = re.search(r':([A-Z_]+)\s*\(', full_process_name)
        process = process_match.group(1) if process_match else full_process_name

        # Cross-reference with task_outcomes: skip if the task eventually
        # succeeded (e.g. a later retry completed after the ignore)
        outcome = task_outcomes.get(full_process_name)
        if outcome and outcome.final_status != "FAILED":
            continue

        errors.append({
            "sample": sample,
            "process": process,
            "exit_code": exit_code,
            "retry_count": outcome.attempts - 1 if outcome else 0,
            "error_message": None,  # Will be populated from .command.err if found
            "native_id": outcome.last_native_id if outcome else None,
        })

    return pipeline, errors


def get_error_from_workdir(work_dir: str) -> Optional[str]:
    """Extract error message from task's .command.err file."""
    if not work_dir:
        return None

    err_file = Path(work_dir) / ".command.err"
    if not err_file.exists():
        return None

    try:
        with open(err_file) as f:
            content = f.read().strip()

        # Return last few lines if content is long
        lines = content.split('\n')
        if len(lines) > 5:
            return '\n'.join(lines[-5:])
        return content if content else None
    except Exception:
        return None


def find_error_workdir(outdir: Path, process: str, sample: str) -> Optional[str]:
    """Find work directory for a failed task by searching logs."""
    log_path = outdir / "logs" / "nextflow.log"
    if not log_path.exists():
        return None

    # Search for the work directory in the log
    pattern = re.compile(
        rf'submitted process.*{process}.*{re.escape(sample)}.*workDir:\s*(\S+)',
        re.IGNORECASE
    )

    with open(log_path) as f:
        content = f.read()

    matches = list(pattern.finditer(content))
    if matches:
        # Return the most recent match
        return matches[-1].group(1)

    return None


def get_slurm_jobs(user: str, job_prefix: str) -> list[dict]:
    """Get currently running/pending SLURM jobs."""
    jobs = []

    try:
        result = subprocess.run(
            ['squeue', '-u', user, '-o', '%i|%j|%T|%M|%N|%r'],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            return jobs

        lines = result.stdout.strip().split('\n')
        if len(lines) < 2:
            return jobs

        for line in lines[1:]:  # Skip header
            parts = line.split('|')
            if len(parts) >= 6:
                job_name = parts[1]
                if job_prefix and not job_name.startswith(job_prefix):
                    continue

                jobs.append({
                    "job_id": parts[0],
                    "name": job_name,
                    "state": parts[2],
                    "time": parts[3],
                    "node": parts[4],
                    "reason": parts[5] if parts[5] != "(null)" else None,
                })
    except Exception as e:
        pass

    return jobs


def get_slurm_job_accounting(job_ids: list[str]) -> dict[str, dict]:
    """Query sacct for SLURM accounting data on completed jobs.

    Batch-queries sacct for the given SLURM job IDs and returns failure
    reason and peak memory usage for each.

    Args:
        job_ids: List of SLURM job ID strings to query.

    Returns:
        Dict keyed by job ID with keys:
            slurm_state: str  (e.g. "OUT_OF_MEMORY", "TIMEOUT", "FAILED", "COMPLETED")
            peak_rss: str|None  (human-readable, e.g. "94.3 GB")
    """
    if not job_ids:
        return {}

    try:
        result = subprocess.run(
            [
                'sacct', '-j', ','.join(job_ids),
                '--format=JobID,State,MaxRSS',
                '--noheader', '--parsable2',
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if result.returncode != 0:
            return {}
    except Exception:
        return {}

    # Parse output: main job line (no '.' in JobID) gives State;
    # '.batch' substep gives MaxRSS
    states: dict[str, str] = {}
    max_rss: dict[str, str] = {}

    for line in result.stdout.strip().split('\n'):
        if not line:
            continue
        parts = line.split('|')
        if len(parts) < 3:
            continue

        job_id_field = parts[0].strip()
        state_field = parts[1].strip()
        rss_field = parts[2].strip()

        if '.' not in job_id_field:
            # Main job line — extract state (strip trailing modifiers like
            # "CANCELLED by ..." but keep core state)
            state = state_field.split()[0] if state_field else ""
            states[job_id_field] = state
        elif job_id_field.endswith('.batch'):
            # Batch substep — extract MaxRSS
            base_id = job_id_field.split('.')[0]
            if rss_field:
                max_rss[base_id] = rss_field

    # Build result dict with human-readable peak_rss
    accounting: dict[str, dict] = {}
    for job_id in job_ids:
        if job_id not in states:
            continue
        rss_str = max_rss.get(job_id)
        peak_rss = _format_rss(rss_str) if rss_str else None
        accounting[job_id] = {
            "slurm_state": states[job_id],
            "peak_rss": peak_rss,
        }

    return accounting


def _format_rss(rss_str: str) -> Optional[str]:
    """Convert sacct MaxRSS value (e.g. '98765432K') to human-readable form.

    sacct reports MaxRSS with a suffix of K (kibibytes), M, G, or T.
    Returns a string like '94.3 GB' or None if parsing fails.
    """
    rss_str = rss_str.strip()
    if not rss_str:
        return None

    multipliers = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
    suffix = rss_str[-1].upper()
    if suffix in multipliers:
        try:
            value_bytes = float(rss_str[:-1]) * multipliers[suffix]
        except ValueError:
            return rss_str
    else:
        try:
            value_bytes = float(rss_str)
        except ValueError:
            return rss_str

    # Convert to most appropriate unit
    gb = value_bytes / (1024**3)
    if gb >= 1.0:
        return f"{gb:.1f} GB"
    mb = value_bytes / (1024**2)
    if mb >= 1.0:
        return f"{mb:.1f} MB"
    return f"{value_bytes / 1024:.1f} KB"


def parse_console_output(log_path: Path) -> dict:
    """Parse the console output log for current progress."""
    progress = {
        "processes": {},
        "samples_complete": set(),
    }

    # Try to find console output in the log
    if not log_path.exists():
        return progress

    with open(log_path) as f:
        content = f.read()

    # Find "Manta SV calling complete" messages
    complete_pattern = re.compile(r'Manta SV calling complete:\s*(\S+)\s*->')
    for match in complete_pattern.finditer(content):
        progress["samples_complete"].add(match.group(1))

    # Parse process status lines like:
    # [hash] process_name | X of Y, cached: Z
    process_pattern = re.compile(
        r'\[([^\]]+)\]\s+(\S+)\s+\|\s+(\d+)\s+of\s+(\d+)(?:,\s*cached:\s*(\d+))?(?:,\s*retries:\s*(\d+))?'
    )

    # Get the last occurrence of each process
    process_status = {}
    for match in process_pattern.finditer(content):
        hash_id = match.group(1)
        process_name = match.group(2)
        completed = int(match.group(3))
        total = int(match.group(4))
        cached = int(match.group(5)) if match.group(5) else 0
        retries = int(match.group(6)) if match.group(6) else 0

        # Clean up process name (remove truncation artifacts)
        process_name = re.sub(r'^fro…', 'from_fastq:', process_name)
        process_name = re.sub(r'…', '', process_name)

        process_status[process_name] = {
            "completed": completed,
            "total": total,
            "cached": cached,
            "retries": retries,
        }

    progress["processes"] = process_status

    return progress


def determine_sample_status(
    tasks: dict[str, TaskInfo],
    task_outcomes: dict[str, TaskOutcome],
    complete_samples: set[str],
    all_samples: set[str],
) -> dict[str, SampleStatus]:
    """Determine the current status of each sample.

    Uses *task_outcomes* (resolved final outcomes) to decide whether a sample
    has permanently failed.  A sample is only marked as failed if at least one
    of its logical tasks has ``final_status == "FAILED"`` — transient retries
    that eventually succeeded are not counted.
    """
    sample_status = {}

    # Group tasks by sample
    tasks_by_sample = defaultdict(list)
    for task in tasks.values():
        if task.sample:
            # Normalize sample name
            sample = re.sub(r'_(tumor|normal).*$', '', task.sample)
            tasks_by_sample[sample].append(task)

    # Group task outcomes by (normalized) sample for efficient lookup
    outcomes_by_sample: dict[str, list[TaskOutcome]] = defaultdict(list)
    for outcome in task_outcomes.values():
        if outcome.sample:
            outcomes_by_sample[outcome.sample].append(outcome)

    for sample in all_samples:
        status = SampleStatus(sample_id=sample)

        # Check for permanently failed tasks first — a sample with any
        # permanently-failed task is "failed" even if other paths (e.g.
        # Manta) completed successfully.
        failed_outcomes = [
            o for o in outcomes_by_sample.get(sample, [])
            if o.final_status == "FAILED"
        ]
        if failed_outcomes:
            status.has_error = True
            worst = failed_outcomes[0]
            status.error_details = (
                f"Exit code {worst.last_exit_code} in {worst.process}"
            )
            status.retry_count = max(
                o.attempts - 1 for o in failed_outcomes
            )

        if not status.has_error and sample in complete_samples:
            status.current_stage = "COMPLETE"
            status.stages_complete = STAGE_ORDER.copy()
        else:
            sample_tasks = tasks_by_sample.get(sample, [])

            # Find completed stages
            completed_processes = set()
            latest_stage = None
            latest_stage_idx = -1
            has_running = False

            for task in sample_tasks:
                if task.status in ('COMPLETED', 'CACHED'):
                    completed_processes.add(task.process)
                elif task.status == 'RUNNING':
                    has_running = True
                    if task.process in STAGE_ORDER:
                        idx = STAGE_ORDER.index(task.process)
                        if idx > latest_stage_idx:
                            latest_stage_idx = idx
                            latest_stage = task.process

            status.stages_complete = [s for s in STAGE_ORDER if s in completed_processes]

            if has_running and latest_stage:
                status.current_stage = f"{latest_stage} (running)"
            elif status.stages_complete:
                # Find next expected stage
                last_complete_idx = max(
                    STAGE_ORDER.index(s) for s in status.stages_complete
                    if s in STAGE_ORDER
                )
                if last_complete_idx < len(STAGE_ORDER) - 1:
                    status.current_stage = status.stages_complete[-1]
                else:
                    status.current_stage = "COMPLETE"
            else:
                status.current_stage = "PENDING"

        sample_status[sample] = status

    return sample_status


def generate_summary(
    outdir: Path,
    user: str,
    job_prefix: str,
    samples: Optional[list[str]] = None,
) -> dict:
    """Generate complete pipeline status summary."""

    trace_path = outdir / "logs" / "execution_trace.txt"
    log_path = outdir / "logs" / "nextflow.log"

    # Parse all data sources
    tasks, task_outcomes = parse_execution_trace(trace_path)
    pipeline, errors = parse_nextflow_log(log_path, task_outcomes=task_outcomes)
    slurm_jobs = get_slurm_jobs(user, job_prefix)
    console_progress = parse_console_output(log_path)

    # Get error details from work directories
    for error in errors:
        work_dir = find_error_workdir(outdir, error['process'], error['sample'])
        if work_dir:
            error['error_message'] = get_error_from_workdir(work_dir)
            error['work_dir'] = work_dir

    # Enrich errors with SLURM accounting data (failure reason, peak memory)
    native_ids = [
        e['native_id'] for e in errors if e.get('native_id')
    ]
    if native_ids:
        accounting = get_slurm_job_accounting(native_ids)
        for error in errors:
            nid = error.get('native_id')
            if nid and nid in accounting:
                error['slurm_state'] = accounting[nid].get('slurm_state')
                error['peak_rss'] = accounting[nid].get('peak_rss')

    # Determine all samples
    all_samples = set()
    if samples:
        all_samples = set(samples)
    else:
        # Extract from tasks
        for task in tasks.values():
            if task.sample:
                sample = re.sub(r'_(tumor|normal).*$', '', task.sample)
                all_samples.add(sample)

    complete_samples = console_progress.get("samples_complete", set())
    sample_status = determine_sample_status(
        tasks, task_outcomes, complete_samples, all_samples,
    )

    # Calculate cached statistics
    cached_tasks = [t for t in tasks.values() if t.cached]
    cached_by_process = defaultdict(int)
    for task in cached_tasks:
        cached_by_process[task.process] += 1

    # Build summary
    summary = {
        "pipeline": {
            "status": pipeline.status,
            "started": pipeline.started,
            "duration": pipeline.duration,
            "completed_at": pipeline.completed_at,
            "work_dir": pipeline.work_dir,
        },
        "task_summary": {
            "total": len(tasks),
            "succeeded": pipeline.succeeded,
            "cached": pipeline.cached,
            "failed": pipeline.failed,
            "retries": pipeline.retries,
        },
        "samples": {
            "total": len(all_samples),
            "complete": sorted(list(complete_samples)),
            "in_progress": {},
            "failed": [],
        },
        "cached_tasks": {
            "total": len(cached_tasks),
            "by_process": dict(cached_by_process),
        },
        "active_jobs": {
            "count": len(slurm_jobs),
            "jobs": slurm_jobs,
        },
        "errors": errors,
        "process_progress": console_progress.get("processes", {}),
    }

    # Categorize samples — errors take precedence over completion
    for sample_id, status in sample_status.items():
        if status.has_error:
            summary["samples"]["failed"].append({
                "sample": sample_id,
                "stage": status.current_stage,
                "error": status.error_details,
                "retries": status.retry_count,
            })
        elif status.current_stage == "COMPLETE":
            if sample_id not in summary["samples"]["complete"]:
                summary["samples"]["complete"].append(sample_id)
        else:
            summary["samples"]["in_progress"][sample_id] = status.current_stage

    summary["samples"]["complete"] = sorted(summary["samples"]["complete"])

    return summary


def format_output(summary: dict, format: str) -> str:
    """Format summary for output."""
    if format == "json":
        return json.dumps(summary, indent=2, default=str)
    elif format == "yaml":
        return yaml.dump(summary, default_flow_style=False, sort_keys=False)
    else:  # markdown
        lines = []
        lines.append("# Pipeline Status")
        lines.append("")

        p = summary["pipeline"]
        lines.append(f"**Status**: {p['status']}")
        if p['started']:
            lines.append(f"**Started**: {p['started']}")
        if p['duration']:
            lines.append(f"**Duration**: {p['duration']}")
        lines.append("")

        # Task summary
        t = summary["task_summary"]
        lines.append("## Task Summary")
        lines.append(f"- Total: {t['total']}")
        lines.append(f"- Succeeded: {t['succeeded']}")
        lines.append(f"- Cached: {t['cached']}")
        lines.append(f"- Failed: {t['failed']}")
        lines.append(f"- Retries: {t['retries']}")
        lines.append("")

        # Sample progress
        s = summary["samples"]
        lines.append("## Sample Progress")
        lines.append(f"- Complete: {len(s['complete'])}/{s['total']}")

        if s['complete']:
            lines.append(f"  - {', '.join(s['complete'][:10])}" +
                        (f" (+{len(s['complete'])-10} more)" if len(s['complete']) > 10 else ""))

        if s['in_progress']:
            lines.append(f"- In Progress: {len(s['in_progress'])}")
            for sample, stage in s['in_progress'].items():
                lines.append(f"  - {sample}: {stage}")

        if s['failed']:
            lines.append(f"- Failed: {len(s['failed'])}")
            for f in s['failed']:
                lines.append(f"  - {f['sample']}: {f['stage']} ({f['error']})")
        lines.append("")

        # Cached tasks
        c = summary["cached_tasks"]
        if c['total'] > 0:
            lines.append("## Cached Tasks")
            lines.append(f"Total cached: {c['total']}")
            for process, count in c['by_process'].items():
                lines.append(f"- {process}: {count}")
            lines.append("")

        # Active jobs
        j = summary["active_jobs"]
        if j['count'] > 0:
            lines.append("## Active SLURM Jobs")
            lines.append(f"Count: {j['count']}")
            for job in j['jobs'][:10]:
                lines.append(f"- {job['job_id']}: {job['name']} ({job['state']}, {job['time']})")
            lines.append("")

        # Errors
        if summary["errors"]:
            lines.append("## Errors")
            for e in summary["errors"]:
                lines.append(f"### {e['sample']} - {e['process']}")
                lines.append(f"- Exit code: {e['exit_code']}")
                if e.get('slurm_state'):
                    lines.append(f"- SLURM state: {e['slurm_state']}")
                if e.get('peak_rss'):
                    lines.append(f"- Peak RSS: {e['peak_rss']}")
                lines.append(f"- Retries: {e['retry_count']}")
                if e.get('error_message'):
                    lines.append(f"- Error: ```{e['error_message'][:200]}```")
                lines.append("")

        return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Check Nextflow pipeline status",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --outdir /path/to/output
  %(prog)s --outdir /path/to/output --format yaml
  %(prog)s --outdir /path/to/output --format json | jq '.samples.complete'
        """
    )

    parser.add_argument(
        "--outdir", "-o",
        required=True,
        type=Path,
        help="Pipeline output directory (contains logs/nextflow.log)",
    )
    parser.add_argument(
        "--format", "-f",
        choices=["yaml", "json", "markdown"],
        default="yaml",
        help="Output format (default: yaml)",
    )
    parser.add_argument(
        "--user", "-u",
        default=os.environ.get("USER", ""),
        help="SLURM user for job query (default: current user)",
    )
    parser.add_argument(
        "--job-prefix", "-j",
        default="nf-",
        help="SLURM job name prefix filter (default: nf-)",
    )
    parser.add_argument(
        "--samples", "-s",
        nargs="+",
        help="List of expected sample IDs (optional)",
    )

    args = parser.parse_args()

    if not args.outdir.exists():
        print(f"Error: Output directory does not exist: {args.outdir}", file=sys.stderr)
        sys.exit(1)

    summary = generate_summary(
        outdir=args.outdir,
        user=args.user,
        job_prefix=args.job_prefix,
        samples=args.samples,
    )

    output = format_output(summary, args.format)
    print(output, flush=True)


if __name__ == "__main__":
    main()
