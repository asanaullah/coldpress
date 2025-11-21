# <sup>COLDPRESS</sup>
 [ Documentation partially generated with AI ]

Coldpress is an extensible optimization and orchestration framework designed to manage complex AI workloads by systematically analyzing and configuring hardware and software states. It functions as a high-level job manager that executes scalable, lifecycle-managed experiments, enabling users to deploy parallel workloads while simultaneously discovering intricate system states (e.g. NUMA topology, network configurations) and dynamically adjusting system configurations and workload parameters. By abstracting the underlying runtime environment, Coldpress provides a unified interface for executing complex testing workflows across diverse infrastructures, including OpenShift, Bare metal, and Slurm, facilitating reproducible research and holistic optimization of the AI technology stack.

Currently, our focus is on AI inference workloads, with a future target being to optimize AI training, as well as other HPC-like workloads.

## Contents
- [Motivation](#motivation)
- [Coldpress Usage Model](#coldpress-usage-model)
- [Quick Start Guide](#quick-start-guide)
  - [Requirements](#requirements)
  - [Setup](#setup)
  - [Launch discovery tasks and experiments](#launch-discovery-tasks-and-experiments)
- [Source Code Structure](#source-code-structure)
- [Coldpress Commands](#coldpress-commands)
  - [System & Job Management](#system--job-management)
  - [Status & Monitoring](#status--monitoring)
  - [Information & File Retrieval](#information--file-retrieval)
  - [Testing (will be removed)](#testing-will-be-removed)



## Motivation
While the widespread adoption of AI has made powerful models accessible to a broad audience, the deep systems expertise required to execute these workloads efficiently remains concentrated within a small group of engineers. Optimizing AI workloads demands a holistic approach where every layer of the technology stack, as shown in the diagram below, presents a unique opportunity to improve outcomes. This outcome could be one or more objectives such as maximum throughput, minimized latency, or energy efficiency.

- **Model & Workload:** At the very top, optimization involves adjusting hyperparameters, quantization levels (e.g., FP16 vs. INT8), and input configurations such as batch sizes, sequence lengths, and concurrency limits to maximize hardware utilization without exhausting memory.

- **Framework & Libraries:** The serving frameworks (like vLLM) and underlying libraries (cuBLAS, NCCL) must be configured for specific parallelism strategies (tensor vs. pipeline parallel) and memory management (e.g., Key-Value cache allocation) to ensure efficient execution on the accelerators.

- **Runtime & OS:** The runtime environment, whether Kubernetes or bare metal, requires precise resource allocation, including CPU core pinning and NUMA-aware scheduling, to reduce latency. Operating system tuning often involves adjusting kernel parameters, enabling hugepages, and isolating cores to prevent context-switching overhead.

- **Hardware & Interconnects:** At the infrastructure level, the physical topology has to be efficiently utilized. This includes ensuring GPUs are paired with the closest NUMA-local NICs to minimize PCIe traversal distance, tuning NIC settings (like MTU and ring buffers) for maximum bandwidth, and configuring switch-level QoS to handle bursty RDMA traffic effectively.

```bash
+-------------------------+------------------------------+
|           Model         |          Workload            |
+-------------------------+------------------------------+
|                       Framework                        |
+--------------------------------------------------------+
|                  Libraries, Compilers                  |
+--------------------------------------------------------+
|        Runtime Environment (baremetal, k8s, ..)        |
+--------------------------------------------------------+
|                         Drivers                        |
+--------------------------------------------------------+
|                    Operating System                    |
+--------------------------------------------------------+
|                        BIOS / UEFI                     |
+--------------------------------------------------------+
|                Hardware (Accelerator, CPU)             |
+--------------------------------------------------------+
|                   PCIe, System Memory                  |
+--------------------------------------------------------+
|                       NIC, Storage                     |
+--------------------------------------------------------+
|                          Switch                        |
+--------------------------------------------------------+
```

The overarching goal of Coldpress is to address this challenge by abstracting the complexities of infrastructure configuration and runtime optimization. For infrastructure configuration, Coldpress codifies the necessary expertise for identifying which system/network characteristics (e.g. PCIe topology, buffer sizes, sleep states) need to be discovered, the methods for discovery, and valid modifications for tuning them. For runtime optimization, Coldpress codifies the expertise on how to manage the lifecycle of AI experiments at scale. This includes launching experiments, monitoring execution, managing storage, collecting results and logs, cleaning up resources upon completion, and maintaining the experiment records needed for future repeatability. This approach facilitates exploration of diverse research objectives, spanning performance optimization through power-efficiency analysis, across the a complex, multi-layered stack.



## Coldpress Usage Model
Coldpress facilitates a dual-role usage model defined by privilege levels. Administrators with privileged access can utilize the framework for infrastructure-level tasks, such as discovering hardware topology and tuning system-wide configurations like kernel parameters. Standard Users, typically operating with restricted access, leverage Coldpress to identify resource availability and optimize workload-specific variables, such as AI model parameters and batch sizes, without altering the underlying system state.
```bash
+---------------------------------------------------------------+
|                            CLUSTER                            |
|             (Nodes, GPUs, Network Fabric, Storage)            |
+---------------------------------------------------------------+
                                ^
                                |
                                v
+---------------------------------------------------------------+
|                       COLDPRESS FRAMEWORK                     |
+---------------------------------------------------------------+
        ^                                               ^
        | (Privileged Access)             (User Access) | 
        v                                               v
+-------+-------+                               +-------+-------+
|     ADMIN     |                               |      USER     |
+---------------+                               +---------------+
|               |                               |               |
| > Discovers:  |                               | > Discovers:  |
|   - Hardware  |                               |   - Resource  |
|     Topology  |                               |     Avail.    |
|   - Network/  |                               |   - Network/  |
|     System    |                               |     System    |
|     State     |                               |     Info      |
|               |                               |               |
| > Tunes:      |                               | > Tunes:      |
|   - System    |                               |   - Batch     |
|     Knobs     |                               |     Size      |
|   - Kernel    |                               |   - Model     |
|     Params    |                               |     Params    |
|   - Network   |                               |   - App       |
|     Config    |                               |     Config    |
|               |                               |               |
+---------------+                               +---------------+

```


## Quick Start Guide
### Requirements
- OS/Environment: Linux environment with access to an OpenShift Cluster.
- Python: Python 3.x (Tested on 3.13).
- OpenShift Client: `oc` CLI tool installed and configured (Tested on version 4.17+).
- Cluster Access: User must be logged in (`oc login`) and have permissions to create/switch to the project named coldpress.
- Environment Variable: `COLDPRESS_ROOT_DIR` must be set to the absolute path of the project root.


### Setup
You must define the root directory for the application to locate configuration files and examples. The following assumes you have cloned the repo and are in the root dir. 

```bash
export COLDPRESS_ROOT_DIR=$(pwd)
```

Install the required python libraries.
```bash
pip install -r requirements.txt
```

Ensure you are logged into your cluster. The tool will automatically attempt to switch to the coldpress project. It is best practice to create it first if it doesn't exist.
```
oc new-project coldpress || oc project coldpress
```

Start the Backend Server by running the main script. This starts the API server on port 50000.
```bash
python coldpress.py
# You should see: Starting Coldpress API server on port 50000...
```


### Launch discovery tasks and experiments 
The Coldpress API listens for POST requests on port 50000. The body must be JSON with a command key.

1. Discover network on `Node 0`. This always launches a job on the `Job 0` queue .
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "discover network 0"}'
```
Expected response
```bash
# Note: "data" will contain the Task ID
{
  "response": {
    "return": {
      "success": true,
      "data": 0
    },
    "stdout": ""
  }
}
```

2. Monitor status of `Job 0` to check if the discovery task is running or completed. The results of network discovery will be placed in `$(COLDPRESS_ROOT_DIR)/system/config/network/<node id>`
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "status 0"}'
```
Expected response
```bash
# Task running
{
  "response": {
    "return": {
      "success": true,
      "data": {
        "running": {
          "job_id": 0,
          "task_id": 0,
          "label": "network discovery for Node: 0 (moc-r4pcc02u15)",
          "msg": "Running"
        },
        "pending": [],
        "completed": [],
        "failed": [],
        "job_status": "running",
        "job_error": null
      }
    },
    "stdout": ""
  }
}

# Task completed
{
  "response": {
    "return": {
      "success": true,
      "data": {
        "running": {
          "job_id": 0,
          "task_id": -1,
          "label": "",
          "msg": "No task running"
        },
        "pending": [],
        "completed": [
          {
            "job_id": 0,
            "task_id": 0,
            "label": "network discovery for Node: 0 (moc-r4pcc02u15)",
            "msg": ""
          }
        ],
        "failed": [],
        "job_status": "running",
        "job_error": null
      }
    },
    "stdout": ""
  }
}

# Note: Job 0 will stay in the "running" state even without any tasks, but all other Jobs will go into a "completed" states once all tasks finish. This is because all discovery tasks are scheduled on the Job 0 queue, and experiments create their own queues (Job 1 onwards).  
```

3. List available experiments.
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "list examples"}'
```
Expected response
```bash
{
  "response": {
    "return": {
      "success": true,
      "data": [
        "yet_another_basic_test",
        "another_basic_test",
        "basic_test"
      ]
    },
    "stdout": ""
  }
}
```
     
4. Launch the basic_test example This creates a new job (should be `Job 1` if run after discovery).
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "launch example basic_test"}'
```
Expected response
```bash
# Note: "data" will contain the Job ID
{
  "response": {
    "return": {
      "success": true,
      "data": 1
    },
    "stdout": ""
  }
}
```

5. Monitor status of `Job 1`
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "status 1"}'
```
Expected response
```bash
# Task 1 running
{
  "response": {
    "return": {
      "success": true,
      "data": {
        "running": {
          "job_id": 1,
          "task_id": 0,
          "label": "vllm server for Node: 0 (moc-r4pcc02u15) using GPU: 0",
          "msg": "Running"
        },
        "pending": [
          {
            "job_id": 1,
            "task_id": 1,
            "label": "Benchmark 1: guidellm run on Node: 0 (moc-r4pcc02u15)",
            "msg": ""
          }
        ],
        "completed": [],
        "failed": [],
        "job_status": "running",
        "job_error": null
      }
    },
    "stdout": ""
  }
}

# Task 1 completed, Task 2 running (Note: Task 1 completed means that the vLLM server is ready, and the server will be automatically shutdown once the Job queue finishes all assigned tasks)
{
  "response": {
    "return": {
      "success": true,
      "data": {
        "running": {
          "job_id": 1,
          "task_id": 1,
          "label": "Benchmark 1: guidellm run on Node: 0 (moc-r4pcc02u15)",
          "msg": "Running"
        },
        "pending": [],
        "completed": [
          {
            "job_id": 1,
            "task_id": 0,
            "label": "vllm server for Node: 0 (moc-r4pcc02u15) using GPU: 0",
            "msg": ""
          }
        ],
        "failed": [],
        "job_status": "running",
        "job_error": null
      }
    },
    "stdout": ""
  }
}

#Once all tasks finish, the "job_status" parameter is set to "completed"
{
  "response": {
    "return": {
      "success": true,
      "data": {
        "running": {
          "job_id": 1,
          "task_id": -1,
          "label": "",
          "msg": "No task running"
        },
        "pending": [],
        "completed": [
          {
            "job_id": 1,
            "task_id": 0,
            "label": "vllm server for Node: 0 (moc-r4pcc02u15) using GPU: 0",
            "msg": ""
          },
          {
            "job_id": 1,
            "task_id": 1,
            "label": "Benchmark 1: guidellm run on Node: 0 (moc-r4pcc02u15)",
            "msg": ""
          }
        ],
        "failed": [],
        "job_status": "completed",
        "job_error": null
      }
    },
    "stdout": ""
  }
}

```

While the Job is active, we can also verify Pods and PVCs using the CLI
```
$ oc get pods -o wide
NAME                                       READY   STATUS    RESTARTS   AGE   IP               NODE             NOMINATED NODE   READINESS GATES
1-0-moc-r4pcc02u15-vllm-inference-server   1/1     Running   0          10m   192.168.50.147   moc-r4pcc02u15   <none>           <none>
1-1-moc-r4pcc02u15-guidellm-benchmark      1/1     Running   0          58s   192.168.50.147   moc-r4pcc02u15   <none>           <none>

$ oc get pvc -o wide
NAME                                                        STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS                           VOLUMEATTRIBUTESCLASS   AGE   VOLUMEMODE
1-1-moc-r4pcc02u15-guidellm-benchmark-oneshot-pvc-mount-0   Bound    pvc-8e962ab7-57af-4a2a-a9cc-678b22858087   100Mi      RWO            ocs-external-storagecluster-ceph-rbd   <unset>                 87s   Filesystem

```

6. Once completed, list results of `Job 1`.
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "list results 1"}'
```
Expected response
```bash
{
  "response": {
    "return": {
      "success": true,
      "data": {
        "-1": {
          "description": "",
          "files": [
            "config.yaml"
          ]
        },
        "1": {
          "description": "Benchmark 1: guidellm run on Node: 0 (moc-r4pcc02u15)",
          "files": [
            "benchmarks.json"
          ]
        }
      }
    },
    "stdout": ""
  }
}
```

7. Print and verify the experiment configuration used for `Job 1`
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "cat 1 -1 config.yaml"}'
```
Expected response
```bash
{
  "response": {
    "return": {
      "success": true,
      "data": "benchmarks:\n  - name: guidellm\n    launch_node: 0 \n    target_node: 0\n    log: True\n    port: 8000\n    args: \n      max-seconds: 30\n      rate-type: throughput\n      data: \"\\\"prompt_tokens=256,output_tokens=128\\\"\"\n\n\nmodel: \n  name: 'ibm-granite/granite-3.3-8b-instruct'\n  max_model_len: 10000\n\nmodel_server:\n  - framework:\n      name: vllm\n      env:\n        VLLM_USE_V1: 1 \n      args:\n        port: 8000\n        gpu-memory-utilization: 0.6\n    hardware:\n      node: 0\n      gpu: 0\n    storage:\n      mounts:\n        - name: s3_main_bucket\n          mount_point: /mnt/s3_data\n          uri: s3://my-project-bucket/models/\n        - name: nfs_shared_fs\n          mount_point: /mnt/nfs_share\n          uri: nfs://10.0.1.5/exports/datasets\n        - name: ceph_cluster_storage\n          mount_point: /mnt/ceph_fs\n          uri: ceph://user@my-ceph-cluster/pools/main_data\n    log: True"
    },
    "stdout": ""
  }
}
```

8. Print the results: `benchmarks.json`
```bash
curl -X POST http://127.0.0.1:50000/ \
     -H "Content-Type: application/json" \
     -d '{"command": "cat 1 1 benchmarks.json"}'
```
Expected response
```bash
{
  "response": {
    "return": {
      "success": true,
      "data": <contents of benchmarks.json>
    },
    "stdout": ""
  }
}
```

## Source Code Structure
```bash
coldpress                            
├── coldpress.py                        # Main entry point, API server, and job orchestration engine
├── README.md                           # Project documentation and usage instructions
├── requirements.txt                    # List of Python dependencies
├── openshift_runtime.py                # Handles OpenShift interactions: pod lifecycle, result extraction, and cleanup 
├── examples                            # Directory containing sample experiment configurations
│   ├── basic_test
│   │   └── config.yaml                 # Sample config: vLLM server (Node 0, GPU 0) + GuideLLM benchmark
│   ├── another_basic_test
│   │   └── config.yaml                 # Sample config: vLLM server (Node 0, GPU 1) + GuideLLM benchmark
│   └── yet_another_basic_test
│       └── config.yaml                 # Sample config: vLLM server (Node 1, GPU 2) + GuideLLM benchmark
├── parsers                             # Modules for parsing configurations into runtime parameters
│   ├── __init__.py                     # Exports parser classes for easier access
│   ├── BenchmarkParser.py              # Parsers for benchmark tools (e.g., GuideLLM)
│   ├── DiscoveryParser.py              # Parsers for system discovery tasks (e.g., network topology)
│   └── vLLMParser.py                   # Parser for vLLM inference server deployment
└── system                              # Stores system-level discovery data and state
└── coldpress_results                   # Stores experiment results
```

## Coldpress Commands
These commands can be submitted to Coldpress by sending POST requests on port 50000. The command itself must be specified using the "command" key in the JSON body of the request (see the Quick Start guide above).

### System & Job Management
1. `discover`: Initiates a discovery task (e.g., network topology) on a specific node.
- Command: discover <discovery_type> <node_id>
  - Example: discover network 0
- Action: Validates the node ID and discovery type, creates a job configuration, and enqueues the discovery task on the system Job (Job 0).
- Output:
  - Success: {"success": True, "data": <task_id>} (Integer)
  - Failure: {"success": False, "data": "Error message string"}

2. `launch`: Parses a configuration file and launches a multi-task job (e.g., model server + benchmarks).
- Command: launch example <example_name>
  - Example: launch example basic_test
- Action: Reads the config.yaml for the specified example, creates a new Job ID (GID), sets up result directories, parses the configuration into tasks (model servers, benchmarks), and enqueues them for execution.
- Output:
  - Success: {"success": True, "data": <gid>} 
  - Failure: {"success": False, "data": "Error message string"}

3. `stop`: Sends a stop signal to a running job.
- Command: stop <gid>
  - Example: stop 1
- Action: Verifies the job is running and sends a termination signal (None sentinel) to the job's worker queue. The job will terminate after the current running task completes.
- Output:
  - Success: {"success": True, "data": "Stop signal sent to job <gid>."}
  - Failure: {"success": False, "data": "Error message string"}

### Status & Monitoring
4. `status`: Retrieves the status of all jobs or details for a specific job.
- Command: status [gid]
  - Option 1 (All Jobs): status (No arguments)
  - Option 2 (Specific Job): status <gid>
- Action:
  - All Jobs: Returns a summary list of all jobs including ID, status, and counts of pending/completed/failed tasks.
  - Specific Job: Returns detailed lists of running, pending, completed, and failed tasks for the specified Job ID.
- Output:
  - All Jobs: {"success": True, "data": {"job_summary": [list of job objects]}}
  - Specific Job: {"success": True, "data": {"running": {...}, "pending": [...], "completed": [...], "failed": [...]}}

5. `isdone`: Checks the completion status of a specific job.
- Command: isdone <gid>
  - Example: isdone 1
- Action: Returns the current status string of the job (e.g., "running", "completed", "failed").
-Output:
  - Success: {"success": True, "data": "<status_string>"}
  - Failure: {"success": False, "data": "Error message string"}

6. `dmesg`: Retrieves the consolidated system console log.
- Command: dmesg
- Action: Returns all log messages accumulated by the Coldpress shell session, sorted chronologically.
- Output: {"success": True, "data": [list of log strings]}

### Information & File Retrieval
7. `list`: Lists various system resources or artifacts.
- Command: list <sub_command> [args]
  - Examples:
    - list examples
    - list nodes
    - list results <gid>
Action:
  - examples: Lists subdirectories in the examples/ folder.
  - nodes: Lists discovered nodes and their GPU availability.
  - results: Lists valid output files for the tasks in the specified Job ID.
- Output:
  - Success: {"success": True, "data": <list_or_dict_of_items>}
  - Failure: {"success": False, "data": "Error message string"}

8. `cat`: Retrieves the content of configuration files or job results.
- Command:
  - cat <example_name>: Reads the config.yaml of an example.
  - cat <gid> <task_id>: Returns the internal task parameter dictionary.
  - cat <gid> <task_id> <filename>: Reads a specific result file from a task's output. (Use task_id -1 to read the job's main config.yaml).
- Action: Resolves the file path safely and reads the content.
- Output:
  - Success: {"success": True, "data": "<file_content_string>"} (or dict for task params)
  - Failure: {"success": False, "data": "Error message string"}

### Testing (will be removed)
9. `vllm_test`: Launches a hardcoded vLLM inference server test on Node 0.
- Command: vllm_test <gpu_id> (GPU id is optional, defaults to 0)
  - Example: vllm_test 1
- Action: Creates a new job specifically for running a vLLM server using the ibm-granite/granite-3.3-8b-instruct model on the specified GPU of Node 0.
- Output:
  - Success: {"success": True, "data": <gid>}
  - Failure: {"success": False, "data": "Error message string"}
    
10. `exit / quit`: Terminates the Coldpress session.
- Command: exit or quit
- Action: Initiates shutdown of the shell and API server.
- Output: {"success": True, "data": ""}
