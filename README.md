# <sup>COLDPRESS</sup>

Coldpress is a Kubernetes-native optimization and orchestration framework designed to manage complex HPC-like workloads such as AI.


## Contents
- [Coldpress Usage Model](#coldpress-usage-model)
- [Coldpress Execution Workflow](#coldpress-execution-workflow)
- [Cluster Setup & Installation](#cluster-setup--installation)
  - [1. Apply CRDs](#1-apply-crds)
  - [2. Setup Kueue](#2-setup-kueue)
  - [3. Deploy Coldpress Operator](#3-deploy-coldpress-operator)
  - [4. Setup Admin Namespace](#4-setup-admin-namespace)
  - [5. Setup User Namespace](#5-setup-user-namespace)
- [Admin Guide: Creating Parsers (Templates)](#admin-guide-creating-parsers-templates)
  - [WorkloadTemplates](#workloadtemplates)
  - [DiscoveryTemplates](#discoverytemplates)
- [User Guide: Submitting Jobs](#user-guide-submitting-jobs)
  - [ColdpressResourceAllocator (Dynamic Scheduling)](#coldpressresourceallocator-dynamic-scheduling)
  - [ComputeJ (Manual Node Targeting)](#computej-manual-node-targeting)

## Coldpress Usage Model
Coldpress facilitates a dual-role usage model defined by Kubernetes namespaces and Role-Based Access Control (RBAC):

* **Administrators (`coldpress-admin`)**: Operate with privileged access. They define `WorkloadTemplates` and `DiscoveryTemplates` (parsers) that dictate how jobs run, what dependencies are needed, and how hardware is accessed. They can also launch privileged `DiscoveryJ` jobs to discover and modify system configuration (PCIe, NUMA, RDMA states).
* **Users (`researcher-a`, etc.)**: Operate in restricted namespaces. They submit `ColdpressResourceAllocator` or `ComputeJ` resources referencing admin-defined templates, supplying only the high-level workload configurations (e.g., batch sizes, model paths).

## Coldpress Execution Workflow

The lifecycle of a Coldpress job involves distinct steps crossing the Admin, User, and System boundaries:

**Setup & Authorization**

1. **Define Templates:** The Administrator defines secure templates (Workload and Discovery).
2. **Set Permissions:** The Administrator sets allowed parsers for the User Namespace via annotations.

**Submission & Processing**

3. **Submit Job:** A Standard User submits a User Job (e.g., ComputeJ, CRA), or the Administrator submits an Admin Job for System Discovery.
4. **Watch CRs:** The Coldpress Operator watches for these new Custom Resources.
5. **Verify Authorization:** The Operator checks the namespace annotations to verify authorization.
6. **Fetch Template:** The Operator fetches the required template.

**Scheduling & Execution**

7. **Score Nodes:** If a ColdpressResourceAllocator (CRA) job is submitted, the Operator queries the Kueue ClusterQueue to evaluate node demand and scoring.
8. **Create JobSet:** The Operator creates a suspended Kubernetes JobSet.
9. **Request Quota:** The JobSet requests quota from Kueue.
10. **Admit Job:** Kueue admits and unsuspends the JobSet.
11. **Schedule Pods:** The JobSet schedules the Pods on the nodes to execute the tasks.

**Storage & Cleanup**

12. **Write Results:** Running Pods write User Results to the User PVC or Admin Results to the Admin PVC.
13. **Trigger Monitor:** Pod completion triggers the Operator's Garbage Collection monitor.
14. **Cleanup:** The Operator cleans up the Kubernetes resources (JobSets) and Custom Resources.


## Requirements
Before starting, ensure your target nodes are labeled with Coldpress IDs (e.g., `oc label node <node-name> coldpress.node=0`). Also create the coldpress namespace where the operator will be deployed (`oc create namespace coldpress`).

Coldpress uses [Kueue](https://kueue.sigs.k8s.io/) and [JobSet](https://jobset.sigs.k8s.io/)  to manage job quotas and queueing. This guide assumes these have been installed on the cluster. 


### 1. Apply CRDs

Coldpress requires five Custom Resource Definitions (CRDs) to be applied to the cluster: `ComputeJ`, `DiscoveryJ`, `WorkloadTemplate`, `DiscoveryTemplate`, and `ColdpressResourceAllocator`.

```bash
oc apply -f system/config/cluster/crds.yaml
```


### 2. Setup Kueue
The next step is to define `ResourceFlavors` (which map to coldpress node labels) and a `ClusterQueue`.  

```bash
oc apply -f system/config/cluster/kueue-init.yaml
```

* **What this does**: It creates `node0` and `node1` flavors, setting nominal quotas for CPUs, Memory, GPUs, and RoCE NICs. It also creates a global `cluster-queue-test` to manage incoming Coldpress jobs.


### 3. Deploy Coldpress Operator

Deploy the Kopf-based Python operator that watches for Coldpress CRDs and translates them into `JobSets`. 

```bash
oc apply -f system/config/cluster/deployment.yaml
```

* **What this does**: Creates a ServiceAccount, ClusterRole bindings with permissions to manage Pods, JobSets, and Kueue resources, and spins up the Operator deployment.

### 4. Setup Admin Namespace

The Admin namespace holds privileged access and stores the globally available templates.

```bash
oc apply -f system/config/admin/admin-setup.yaml
```

* **What this does**:
* Creates the `coldpress-admin` namespace.
* Applies Pod Security Admission labels to allow `privileged` pods.
* Creates a `LocalQueue` to connect to Kueue.
* Sets up persistent storage (`coldpress-admin-storage`) for system discovery results.

### 5. Setup User Namespace

Setup a restricted namespace for researchers/users to submit workloads.

```bash
oc apply -f system/config/user/user-setup.yaml
```

* **What this does**:
* Creates the `researcher-a` namespace.
* Uses annotations (`coldpress.io/allowed-allocator-parsers` and `coldpress.io/allowed-compute-parsers`) to securely restrict *which* templates this user is allowed to execute.
* Provisions standard (non-privileged) storage PVCs and NetworkAttachmentDefinitions (for SR-IOV/RDMA).



## Admin Guide: Creating Parsers (Templates)

Admins codify the execution environment for AI tasks using templates. These templates live in the `coldpress-admin` namespace.

### WorkloadTemplates

`WorkloadTemplates` define a specific application (e.g., vLLM, GuideLLM, GROMACS).

To upload a parser:

```bash
oc apply -f parsers/workload/vllm-parser.yaml
```

**Key Sections of a WorkloadTemplate**:

* `requirements`: Hints for the Allocator regarding how many GPUs or RoCE NICs this template requires per node (e.g., `gpus_per_node: "{num_gpus}"`).
* `user_params`: A list of variable names the user *must* provide in their job YAML (e.g., `model`, `port`).
* `allocator_params`: Variables that the Coldpress operator injects automatically during scheduling.
* `image` & `args`: The container image and command to run. Variables in `{}` brackets are string-interpolated at runtime based on `user_params`.
* `blocking`: Defines task dependencies in multi-task workflows:
  * `type: completion`: The next task waits for this task to exit successfully (e.g., a training script).
  * `type: endpoint`: The next task waits until an HTTP endpoint is reachable (e.g., wait for vLLM `http://127.0.0.1:{port}/health` before starting the benchmark).
  * `type: delay`: Blindly waits N seconds.

* `ephemeral_mounts`: Dynamically provisions the job directory (e.g., `/tmp/result`) to be mounted in order to extract results to the user's permanent PVC upon job completion.

### DiscoveryTemplates

Used for running infrastructure-level discovery (e.g., PCIe topology, system power states). These are executed via a `DiscoveryJ`.

* `script`: The raw bash/python script to run.
* `result_dir`: The directory in the admin PVC where the output will be aggregated.

## User Guide: Submitting Jobs

Users define the sequence of tasks they want to run. There are two primary ways to submit workloads.

### ColdpressResourceAllocator (Dynamic Scheduling)

The `ColdpressResourceAllocator` (CRA) is the recommended way to submit jobs. The user specifies *what* they want to run and its parameters, and the Operator's Kueue integration automatically calculates the node with the lowest load and highest availability to schedule the tasks.

**Example: vLLM + GuideLLM Benchmark**

```yaml
kind: ColdpressResourceAllocator
metadata:
  name: vllm-guidellm
  namespace: researcher-a
spec:
  storage:
    results: researcher-a-storage
  tasks:
    - name: "inference-server"
      template: "vllm-parser"
      params:
        num_gpus: 1
        model: "ibm-granite/granite-3.3-8b-instruct"
        max_model_len: 10000
        port: 8000
        gpu_memory_utilization: 0.6
    - name: "benchmark-run"
      template: "guidellm-parser"
      params:
        target_task: 0
        port: 8000
        max_seconds: 30
        rate_type: "throughput"
        rate: 1
        data: "prompt_tokens=256,output_tokens=128"
```

**How it works**:

1. The user submits this file (`oc apply -f config.yaml`).
2. The Operator reads the templates, verifies permissions, sees Task 0 needs 1 GPU, checks Kueue for the best node, and allocates the job.
3. It spawns a native Kubernetes `JobSet`, managing the `vllm-parser` pod first, and starting the `guidellm-parser` pod only after vLLM's health endpoint goes green.

### ComputeJ (Manual Node Targeting)

If a user explicitly needs to target exact hardware (e.g., benchmarking an RDMA connection between specifically Node 0 and Node 1), they use `ComputeJ`.

**Example: RDMA Perf Test**

```yaml
apiVersion: coldpress.io/v1
kind: ComputeJ
metadata:
  name: roce-test
  namespace: researcher-a
spec:
  storage:
    results: researcher-a-storage
  tasks:
    - name: "perf-server"
      template: "perftest-server"
      node: 0
      params:
        gid_index: 3 
        port: 18515
        sriov_resource_name: "openshift.io/eno5np0rdma" 
        network_name: "sriov-rdma-net-eno5"
        flags: "-m 4096 -q 1 -s 1048576"
        
    - name: "perf-client"
      template: "perftest-client"
      node: 1
      params:
        gid_index: 3
        port: 18515
        sriov_resource_name: "openshift.io/eno5np0rdma"
        network_name: "sriov-rdma-net-eno5"
        flags: "-m 4096 -q 1 -s 1048576"
```

Once submitted, you can monitor execution natively using Kubernetes tools:

```bash
oc get compute-jobs -n researcher-a
oc get jobsets -n researcher-a
oc get pods -n researcher-a

```

Results and benchmark files will automatically be deposited into the `researcher-a-storage` PVC inside a timestamped directory (e.g., `data/coldpress_results/roce-test_20240101_120000/`) upon completion.
