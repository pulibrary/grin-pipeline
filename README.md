# GRIN Pipeline #

## Kanban-Style Workflow ##
The design implements a Kanban-style workflow, where stages are asynchronous processes that communicate with other processes via tokens moved along a sequence of directories (buckets).  This is a common pattern for implementing resilient, loosely coupled pipelines, and it offers several advantages:

  * It is asynchronous.  Each stage runs independently, improving fault isolation
  * It is observable.  You can monitor the state of the pipeline just by looking at the directories.
  * It is restartable.  Crashed stages can resume simply by rechecking their input directories.
  * It is extensible.  It is easy to add more stages or logic by inserting new directories and processors.

## Architecture ##
The pipeline is implemented as a directory tree.  For example,

```
pipeline/
├── 01_incoming/         # encrypted tarballs (downloaded)
├── 02_decrypted/        # decrypted tarballs
├── 03_extracted/        # extracted files
├── 04_modified/         # modified tarballs
├── 05_uploaded/         # post-upload, archiving or success marker
├── logs/                # optional: logs per stage
```

Each process 
  * watches its input_dir for new tokens (files)
  * does its work
  * moves the token to the next directory

An Orchestrator controls the set-up and tear-down of the pipeline.  It reads a configuration file, prepares the buckets, and starts the processes.

Here is a part of an example config.yaml file:
```yaml
global:
  temp_dir: /tmp/pipeline
  gpg_passphrase: "s3cr3t"

filters:
  - name: decryptor
    class: Decryptor
    input_pipe: pipeline/01_incoming
    output_pipe: pipeline/02_decrypted
```
