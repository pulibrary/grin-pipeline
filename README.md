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

## A Workflow Walkthrough ##
User launches the orchestrator, perhaps with a config file passed in, or the config file is at an understood location. The configuration includes the location of an all_books file, which is expensive to generate.

If there are no tokens in the to_dowload bucket, the Orchestrator seeds the pipeline, generating some new tokens (a sample can be passed in on the command line).

What is available but not converted?

Our first pipeline will download and decode all the books that have been converted.


## Anatomy of a Token
  * barcode
  * processing_bucket
  * storage_bucket
  
  e.g,
  ```json
  {
	  "barcode" : "12345",
	  "processing_bucket" : "/var/tmp/processing"
	  "done_bucket" : "/var/tmp/done"
  }
  ```



## Filesystem Setup ##
As above, there must be a pipeline directory for managing the kanban flow.

The data files must go somewhere; the Orchestrator specifies locations when it
creates tokens?


