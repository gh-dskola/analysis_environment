# Code Style Preferences
  - Use descriptive variable names, avoid single letters except for loop indices
  - Prefer functional approaches over mutation when reasonable
  - Write docstrings for public functions

# Python Style Preferences
  - Use type hints
  - Follow PEP8 formatting guidelines
  - Pass arguments explicitly by keyword rather than implicitly by position where possible
  - Use pathlib.Path objects where it makes sense
  - Use the following variable naming conventions for path variables:
    - Variables that are expected to contain a fully-qualified path to a file should end in "_fpath"
    - Variables that are expected to contain a path to a directory should end in "_path"
    - Variables that are expected to contain a filename only (without path information) should end in "_fname"
    - File handles should end in "_file"

# Dynamic Versioning with setuptools-scm

  When setting up a new Python package, use setuptools-scm for automatic versioning from git tags:

  ### 1. Update pyproject.toml

  Add setuptools-scm to build requirements and make version dynamic:

  ```toml
  [build-system]
  requires = ["setuptools>=61.0", "wheel", "setuptools-scm>=8.0"]
  build-backend = "setuptools.build_meta"

  [project]
  name = "package-name"
  dynamic = ["version"]
  # ... rest of project config (do NOT include a static version field)

  [tool.setuptools_scm]
  version_scheme = "guess-next-dev"
  local_scheme = "node-and-date"

  2. Update init.py

  Replace any hardcoded __version__ with dynamic lookup:

  from importlib.metadata import version, PackageNotFoundError

  try:
      __version__ = version("package-name")  # Use the package name from pyproject.toml
  except PackageNotFoundError:
      __version__ = "0.0.0.dev0"

  3. Create initial tag

  After committing the above changes:

  git tag -a v0.1 -m "Initial release v0.1"
  pip install -e .  # Reinstall to pick up version

  Version behavior

  - Tagged commit: 0.1
  - 3 commits after tag: 0.2.dev3+g1234567.d20250128
  - New release: git tag -a v0.2 -m "Release v0.2"

# Time Estimates
Provide time estimates when relevant empirical data is available. I can evaluate them myself.

# Nextflow Work Directory Cleanup                                                                                                                                                                           
                                                                                                                                                                                                              
  When cleaning up Nextflow work directories to free disk space while preserving data needed for pipeline resume:                                                                                             
                                                                
  1. **Define "failed samples" by final output**: A sample is incomplete if it lacks the final expected output (e.g., no Manta VCF). Do NOT define failure by which specific task failed.                     
                                                                                                                                                                                                              
  2. **Preserve the entire dependency graph**: For each incomplete sample, keep ALL work directories for ALL tasks in that sample's pipeline - including successfully completed upstream tasks (e.g., normal
  alignment for a tumor that failed).

  3. **Identify by sample_id, not task hash**: Extract sample_ids without final outputs, then preserve all work directories whose `.command.sh` or output files reference those sample_ids.

  Example approach:
  ```bash
  # Find samples missing final output
  ls results/manta/*.vcf.gz | sed 's/.*\///' | sed 's/_.*$//' | sort -u > completed_samples.txt
  cat samplesheet.csv | cut -d',' -f1 | tail -n+2 | sort -u > all_samples.txt
  comm -23 all_samples.txt completed_samples.txt > incomplete_samples.txt

  # Find work dirs to KEEP (any dir referencing incomplete samples)
  for sample in $(cat incomplete_samples.txt); do
      grep -rl "$sample" work/*/.*command* 2>/dev/null | xargs -I{} dirname {}
  done | sort -u > dirs_to_keep.txt

  # Delete everything else
  find work -mindepth 2 -maxdepth 2 -type d | sort -u > all_work_dirs.txt
  comm -23 all_work_dirs.txt dirs_to_keep.txt > dirs_to_delete.txt

  Key principle: When in doubt, preserve more. Re-running upstream tasks wastes compute; accidentally deleting needed cache costs more than keeping extra directories.
  ```

# Checking Nextflow Pipeline Status

When asked to check the status of a running Nextflow pipeline, use the dedicated status script:

```bash
~/scripts/check_pipeline_status.py --outdir /path/to/pipeline/output --format yaml
```

This provides structured output with:
- **Pipeline status**: running/complete, duration, timestamps
- **Sample progress**: which samples are complete, in-progress, or failed
- **Cached tasks**: what was resumed from cache (by process type)
- **Active SLURM jobs**: currently running jobs with runtime
- **Errors**: failed tasks with exit codes, retry counts, and error messages

Options:
- `--format yaml|json|markdown`: Output format (default: yaml)
- `--user USERNAME`: Filter SLURM jobs by user
- `--job-prefix PREFIX`: Filter SLURM jobs by name prefix (default: nf-)

Example usage:
```bash
# Quick status check
~/scripts/check_pipeline_status.py -o /screening/data/.../nf_output/my_run

# Get just the failed samples as JSON
~/scripts/check_pipeline_status.py -o /path/to/output -f json | jq '.samples.failed'

# Get complete sample list
~/scripts/check_pipeline_status.py -o /path/to/output -f json | jq '.samples.complete'
```

Do NOT use ad-hoc combinations of tail, grep, and squeue to check status. The script provides consistent, accurate information.

# General
* Don't be afraid to push back if you think there's a better (more standard, more robust, more performant, faster) way to do something. Give me your critique and/or suggestions for alternatives and I can accept or reject your idea.
