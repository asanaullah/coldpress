import io
import os
import sys
import copy
import yaml
import time
import json
import queue
import shlex
import shutil
import asyncio
import tempfile
import threading
import contextlib
from io import StringIO
from pathlib import Path
from datetime import datetime
import openshift_client as oc
from contextlib import redirect_stdout
from openshift_runtime import openshift_run as orun
from openshift_runtime import openshift_cleanup as oclean
from http.server import BaseHTTPRequestHandler, HTTPServer
from parsers import (BenchmarkParser,vLLMParser,DiscoveryParser)
from models import ConfigFile
from pydantic import ValidationError

# Labels were added to nodes using coldpress.node=x

# Todo
# fix path create - use os.path.join()
# convert job logging to realtime
# add cleanup for failed tasks
# remove code duplication
# make error handling consistent
# add error checks e.g. if None is returned for a parser call
# fix the copy_cmd in openshift_runtime
# validate that gpu orderings seen at different points is consistent
# add checks in case jobs are getting scheduled for a resource already in use by a lower gid
# better way to pick ip to use for benchmark if launch_node != target_node
# add the validate_config helper function back in 
# check if task is in completed list before running the do_cat function
# guidellm:latest has issues, :nightly works fine - but unable to detect the csv extension for output (worked for baremetal) - using json for now
# can launch multiple jobs in parallel that will run, but guidellm has issues with being run on the same machine twice at the same time
# add in shutdown_all function
# fix demsg
# streamling the cat function so we can get logs as well (or make it part of dmesg?)
# improve job 0 so that it can safely handle potentially disruptive tasks like modifying hardware config that impacts all users
# Support resource request instead of specifying  - instead of prespecifying node, a third party service is asked to get a node (and gpu id), and this is plugged into the example config
# Add in storage support to get larger models, checkpointing, datasets etc 
# End goal - large scale testing on multiple nodes

def set_openshift_project():
    current_project = oc.get_project_name()
    coldpress_project = "coldpress"
    if  current_project != coldpress_project:
        print(f"Switching from project '{current_project}' to '{coldpress_project}'")
        oc.set_project(coldpress_project)
    else:
        print(f"Already in target project: '{coldpress_project}'")


def get_root_dir():
    ROOT_VAR_NAME = "COLDPRESS_ROOT_DIR"
    root_dir = os.getenv(ROOT_VAR_NAME)
    if not root_dir:
        print (
            f"Error: The environment variable '{ROOT_VAR_NAME}' must be defined."
        )
        sys.exit(1)
    if not os.path.isabs(root_dir):
        print (
            f"Error: The path for '{ROOT_VAR_NAME}' must be an absolute path.\n"
            f"  Provided value: '{root_dir}'"
        )
        sys.exit(1)
    if not os.path.isdir(root_dir):
        print(
            f"Error: The directory specified by '{ROOT_VAR_NAME}' does not exist.\n"
            f"  Path not found: '{root_dir}'"
        )
        sys.exit(1)
    return os.path.normpath(root_dir)

class ColdpressShell(object): 
    def __init__(self):
        super().__init__()   
        set_openshift_project()
        self.meta_data = {"timestamp": datetime.now().strftime("%Y_%m_%d_%H_%M_%S"), "tmpdir": '', "root_dir": get_root_dir()}
        self.meta_data["tmpdir"] = f"/tmp/coldpress_tmpdir_{self.meta_data["timestamp"]}"
        os.makedirs(self.meta_data["tmpdir"], exist_ok=False)
        self.parsers = {
            'vllm': vLLMParser(), 
            'benchmark': BenchmarkParser(), 
        #     'router': RouterParser(), 
            'discovery': DiscoveryParser(), 
        #     'logging': LoggingParser()
        }
        self.jobs = []
        self.log = []
        self.resources = {"nodes": {}}
        ret = self.create_job() #job queue 0 is long running that only runs blocking tasks e.g. discover
        nodes = oc.selector('nodes').objects()
        for node in nodes:
            labels = node.model.metadata.get('labels', {})
            if "coldpress.node" in labels:
                nodeid = labels["coldpress.node"]
                allocatable = node.model.status.get('allocatable', {})
                gpu_count_str = allocatable.get('nvidia.com/gpu', '0')
                try:
                    gpu_count = int(gpu_count_str)
                except ValueError:
                    gpu_count = 0
                gpu_availability_map = {}
                for i in range(gpu_count):
                    gpu_id = str(i) 
                    gpu_availability_map[gpu_id] = False 
                self.resources["nodes"][str(nodeid)] = {
                    "name": node.name(),
                    "gpus": gpu_availability_map
                }        
        print("Coldpress Nodes:")
        print(self.resources["nodes"])

    def log_msg(self, msg):
        now = datetime.now()
        timestamp_tag = now.strftime("%Y%m%d%H%M%S%f")
        self.log.append(f'[{timestamp_tag}] [Console] {msg}')

    async def job_worker(self, job):
        def log_job_msg(job, msg):
            gid = job.get("gid", -1)
            now = datetime.now()
            timestamp_tag = now.strftime("%Y%m%d%H%M%S%f")
            with job["lock"]:
                job["log"].append(f'[{timestamp_tag}] [Job {gid}] {msg}')
        def flush_queue(job):
            while not job["job_queue"].empty():
                try:
                    job["job_queue"].get_nowait()
                    job["job_queue"].task_done()
                except asyncio.QueueEmpty:
                    break
            log_job_msg(job, f"Job queue has been flushed due to a failure.")
        log_job_msg(job, "Job worker is listening for tasks...")
        while True:
            task = await job["job_queue"].get()
            if task is None: # Use None as a sentinel value to stop - THIS IS IMPORTANT FOR CLEANUP OF JOB
                break
            log_job_msg(job, f"Dequeued task #{task['id']}: {task['label']}")
            with job["lock"]:
                job["running_task"] = task
            stdout_buffer = io.StringIO()
            stderr_buffer = io.StringIO()
            try:
                method_to_call = task['method']
                with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
                    if asyncio.iscoroutinefunction(method_to_call):
                        ret = await method_to_call(*task['args'], **task['kwargs'])
                    else:
                        ret = method_to_call(*task['args'], **task['kwargs'])
                with job["lock"]:
                    task['status'] = 'completed'
                    job["completed_tasks"].append(task)
                    job["running_task"] = ''
                    job["active_resources"].append({"task_id": task['id'], "resources": ret})
                log_job_msg(job, f"Task #{task['id']} completed successfully.")
            except Exception as e:
                with job["lock"]:
                    job["status"] = "failed"
                    job["error"] = str(e)
                    job["failed_tasks"].append(task)
                log_job_msg(job, f"FATAL: Task #{task['id']} failed: {e}")
                flush_queue(job)
                log_job_msg(job, f"System shutdown initiated due to job failure.")
                break
            finally:
                stdout_val = stdout_buffer.getvalue().strip()
                stderr_val = stderr_buffer.getvalue().strip()
                if stdout_val:
                    log_job_msg(job, f"[STDOUT] --- Task {task['id']} Output ---")
                    for line in stdout_val.split('\n'):
                        log_job_msg(job, f"[STDOUT] {line}")
                if stderr_val:
                    log_job_msg(job, f"[STDERR] --- Task {task['id']} Output ---")
                    for line in stderr_val.split('\n'):
                        log_job_msg(job, f"[STDERR] {line}")
                job["job_queue"].task_done()
        stdout_buffer.seek(0)
        stdout_buffer.truncate(0)
        stderr_buffer.seek(0)
        stderr_buffer.truncate(0)
        for entry in job["active_resources"]:
            task_id = entry["task_id"]
            resources = entry["resources"]
            if not resources:
                continue
            params = {}
            for task in job["completed_tasks"]:
                if task["id"] == task_id:
                    params = task['args'][0]
                    break
            if not params:
                continue
            with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(stderr_buffer):
                oclean(params, resources)    
            stdout_val = stdout_buffer.getvalue().strip()
            stderr_val = stderr_buffer.getvalue().strip()
            if stdout_val:
                log_job_msg(job, f"[STDOUT] --- Cleanup for Task {task_id} Output ---")
                for line in stdout_val.split('\n'):
                    log_job_msg(job, f"[STDOUT] {line}")
            if stderr_val:
                log_job_msg(job, f"[STDERR] --- Cleanup for Task {task_id} Output ---")
                for line in stderr_val.split('\n'):
                    log_job_msg(job, f"[STDERR] {line}")
        with job["lock"]:
            job["status"] = "completed"

    def start_job_queue_worker(self, job):
        def run_loop():
            job["loop"] = asyncio.new_event_loop()
            asyncio.set_event_loop(job["loop"])
            job["started_event"].set()
            job["loop"].run_until_complete(self.job_worker(job))
        job["worker_thread"] = threading.Thread(target=run_loop, daemon=True)
        job["worker_thread"].start()

    def create_job(self):
        gid = len(self.jobs) 
        job = {
            "gid": gid,
            "job_queue": asyncio.Queue(),
            "tasks": 0,
            "completed_tasks": [],
            "failed_tasks": [],
            "result_dir": None,
            "loop": None,
            "worker_thread": None,
            "running_task": "",
            "log": [],
            "status": "running", 
            "error": None,  
            "started_event": threading.Event(),
            "lock": threading.Lock(),
            "active_resources": [],
            "task_list": []
        }
        self.jobs.append(job)
        self.start_job_queue_worker(job)
        self.log_msg(f"Job {gid} created and asynchronous job worker started.")
        return gid


    def enqueue_task(self, job_gid, label, method_name, *args, **kwargs):
        job = self.jobs[job_gid]
        if not job["worker_thread"].is_alive():
            self.log_msg(f"Error: Job worker for job # {job['gid']} is not running. Cannot enqueue task.")
            return
        job["started_event"].wait()
        with job["lock"]:
            if job["status"] != "running":
                self.log_msg(f"Error: Job {job_gid} is not running (status: {job['status']}). Cannot enqueue.")
                return
            task_id = job["tasks"]
            job["tasks"] += 1
        task = {
            "id": task_id,
            "label": label,
            "method": method_name,
            "args": args,
            "kwargs": kwargs,
            "status": "pending"
        }
        job["loop"].call_soon_threadsafe(job["job_queue"].put_nowait, task)
        self.log_msg(f"Enqueued task #{job['gid']}-{task_id}: {task['label']}")
        return task_id

    def do_dmesg(self, arg):
        mega_list = []
        mega_list.extend(self.log)
        return {"success": True, "data": sorted(mega_list)}

    def do_discover(self, arg):
        args = shlex.split(arg)
        if not args:
            return {"success": False, "data": "Error: Incomplete discover command."}
        try:
            discovery_type = args[0].lower()
            target_node = args[1].lower()
            if str(target_node) not in self.resources["nodes"].keys():
                return {"success": False, "data": "Invalid nodes."}
        except: 
            return {"success": False, "data": "Error: Incomplete discover command."}
        config = {}
        if discovery_type == "network":
            params = {
                "run_params": self.parsers["discovery"].parse(discovery_type=discovery_type, config=config),
                "node_id": str(target_node),
                "node_name": self.resources["nodes"][str(target_node)]["name"],
                "tmpdir": f"{self.meta_data["tmpdir"]}/0/discover",
                "target_dir": self.meta_data["root_dir"] + "/system/config/network/" +  target_node,
                "tag": "0-discover" #we are not sure what the task id will be in job 0, but it only runs blocking tasks so we can use func name as tag
            }
            if params.get("run_params",{}).get("blocking",{}).get("type", '') != "completion":
                return {"success": False, "data": "Error: Unsupported blocking type for network discovery - this is a bug in the tool, not the user input."}
            with self.jobs[0]["lock"]:
                self.jobs[0]["task_list"].append({"label": f"network discovery for Node: {params["node_id"]} ({params["node_name"]})", "params": params} )
            task_id = self.enqueue_task(0, f"network discovery for Node: {params["node_id"]} ({params["node_name"]})", orun, params)
            return {"success": True, "data": task_id}
        else:
            return {"success": False, "data": "Error: Unsupported discovery type."}

    def do_stop(self, arg):
        try:
            gid = int(arg)
            if gid < 1 or gid >= len(self.jobs):
                return {"success": False, "data": f"Error: Invalid job ID: {gid}."}
            job = self.jobs[gid]
            with job["lock"]:
                if job["status"] != "running":
                    return {"success": False, "data": f"Error: Job {gid} is already stopped (status: {job['status']})."}
                if not job["worker_thread"] or not job["worker_thread"].is_alive():
                    return {"success": False, "data": f"Error: Job {gid} worker is not running."}
            job["loop"].call_soon_threadsafe(job["job_queue"].put_nowait, None)
            self.log_msg(f"Sent stop signal to job {gid}. It will clean up and terminate after its current task.")
            return {"success": True, "data": f"Stop signal sent to job {gid}."}
        except (ValueError, TypeError):
            return {"success": False, "data": f"Error: Invalid job ID '{arg}'. Must be an integer."}
        except Exception as e:
            return {"success": False, "data": f"An error occurred: {e}"}

    def do_vllm_test(self, arg):
        gpu = int(arg) if arg else 0
        try:
            target_node = 0
            gid = self.create_job()
            config = {
                    "server_config": {
                        "framework":{
                            "name": "vllm",
                            "env": {
                                "VLLM_USE_V1": 1
                            },
                            "args": {
                                "port": 8000,
                                "gpu_memory_utilization": 0.6
                            }
                        },
                        "hardware": {"node": target_node, "gpu": gpu},
                        "log": True
                    }, 
                    "model_config": 
                        {"model": 'ibm-granite/granite-3.3-8b-instruct', 
                        "max_model_len": 10000
                        }
                    }
            params = {
                    "run_params": self.parsers["vllm"].parse(config=config),
                    "node_id": str(target_node),
                    "node_name": self.resources["nodes"][str(target_node)]["name"],
                    "tmpdir": f"{self.meta_data["tmpdir"]}/{gid}/0",
                    "target_dir": self.meta_data["root_dir"] + "/coldpress_results/" +  str(target_node),
                    "tag": f"{gid}-{0}"
                }
            with self.jobs[gid]["lock"]:
                self.jobs[gid]["task_list"].append({"label": f"vllm server for Node: {params["node_id"]} ({params["node_name"]})", "params": params} )
            task_id = self.enqueue_task(gid, f"vllm server for Node: {params["node_id"]} ({params["node_name"]})", orun, params)
            return {"success": True, "data": gid}
        except Exception as e:
            return {"success": False, "data": f"An error occurred: {e}"}


#ALSO have a dump_params function that is scheduled just before the None task. this will dump out the params for each task in its results folder
# every task launched 0 or 1 pods. so as long as pods have a prefix of {gid}-{task_id}, the naming will always be unique
    def do_launch(self, arg):
        try:
            args = shlex.split(arg)
            if not args:
                return {"success": False, "data": "Error: Please specify a command."}
            cmd = args[0].lower()
            if cmd == "example":
                example = args[1].lower()
                config_file = f"{self.meta_data["root_dir"]}/examples/{example}/config.yaml"
                if not os.path.isfile(config_file):
                    return {"success": False, "data": f"Error: Cannot find the configuration for {example}."}
                try:
                    with open(config_file, "r") as f:
                        raw_config = yaml.safe_load(f)
                        validated_config = ConfigFile.model_validate(raw_config)
                        config = validated_config.model_dump()
                except ValidationError as e:
                    return {"success": False, "data": f"Configuration validation failed: {e}"}
                result_dir_base = os.path.join(self.meta_data["root_dir"] , "coldpress_results", f"results_{self.meta_data["timestamp"]}")
                gid = self.create_job()
                os.makedirs(f"{result_dir_base}/{gid}", exist_ok=True)
                self.log_msg(f"Result directory: {result_dir_base}/{gid}")
                job = self.jobs[gid]
                with job["lock"]:
                    job["result_dir"] = f"{result_dir_base}/{gid}"
                task_id = 0
                task_list = []
                for server_config in config["model_server"]:
                    framework = server_config["framework"]["name"]
                    target_node = server_config["hardware"]["node"]
                    combined_config = {"model_config": config["model"], "server_config": server_config}
                    params = {
                        "run_params": self.parsers[framework].parse(config=combined_config),
                        "node_id": str(target_node),
                        "node_name": self.resources["nodes"][str(target_node)]["name"],
                        "tmpdir": f"{self.meta_data["tmpdir"]}/{gid}/{task_id}",
                        "target_dir": f"{result_dir_base}/{gid}/{task_id}",
                        "tag": f"{gid}-{task_id}"
                    }
                    gpu = server_config.get("hardware", {}).get("gpu", 0)
                    task_list.append({"label": f"{framework} server for Node: {params["node_id"]} ({params["node_name"]}) using GPU: {gpu}", "params": params} )
                    task_id += 1
                for benchmark_id, benchmark in enumerate(config["benchmarks"]):
                    name = benchmark["name"]
                    launch_node = benchmark["launch_node"]
                    target_node = benchmark["target_node"]
                    if launch_node == target_node:
                        target_nodeip = "127.0.0.1"
                    else:
                        with open(f"{self.meta_data["root_dir"]}/system/config/network/{target_node}/network_discovery.yaml") as f:
                            network = yaml.safe_load(f)    
                        target_nodeip = network["ip"]["data"]["gpu_0"]["inet"]
                    benchmark["target_nodeip"] = target_nodeip
                    params = {
                        "run_params": self.parsers["benchmark"].parse(benchmark=name, config=benchmark),
                        "node_id": str(launch_node),
                        "node_name": self.resources["nodes"][str(launch_node)]["name"],
                        "tmpdir": f"{self.meta_data["tmpdir"]}/{gid}/{task_id}",
                        "target_dir": f"{result_dir_base}/{gid}/{task_id}",
                        "tag": f"{gid}-{task_id}"
                    }
                    task_list.append({"label": f"Benchmark {benchmark_id+1}: {name} run on Node: {params["node_id"]} ({params["node_name"]})", "params": params} )
                    task_id += 1
                # CAN RUN CHECKS HERE FOR RESOURCE CONFLICTS BEFORE LAUNCHING
                with job["lock"]:
                    job["task_list"] = task_list
                job_info = {
                    "nodes": {node_id: {"name": node_data["name"], "gpu_count": len(node_data["gpus"])} for node_id, node_data in self.resources["nodes"].items()},
                    "task_list": task_list
                }
                with open(f"{result_dir_base}/{gid}/meta_data.yaml", 'w') as f:
                    yaml.dump(
                        job_info, 
                        f, 
                        sort_keys=False, 
                        default_flow_style=False
                    )
                network_dump = {}
                for node_id in self.resources["nodes"].keys():
                    try:
                        with open(f"{self.meta_data["root_dir"]}/system/config/network/{node_id}/network_discovery.yaml") as f:
                            network = yaml.safe_load(f) 
                        network_dump[node_id] = network
                    except:
                        self.log_msg(f"No network data found for Node {node_id} in {self.meta_data["root_dir"]}/system/config/network/")
                        continue
                with open(f"{result_dir_base}/{gid}/network.yaml", 'w') as f:
                    yaml.dump(
                        network_dump, 
                        f, 
                        sort_keys=False, 
                        default_flow_style=False
                    )
                shutil.copy(f"{self.meta_data["root_dir"]}/examples/{example}/config.yaml", f"{result_dir_base}/{gid}/config.yaml")
                for task in task_list:
                    self.enqueue_task(gid, task["label"], orun, task["params"])
                self.log_msg(f"All tasks for job {gid} enqueued. Sending stop sentinel.")
                job["loop"].call_soon_threadsafe(job["job_queue"].put_nowait, None)
                return {"success": True, "data": gid}
            else:
                return {"success": False, "data": "Error: Invalid launch command."}
        except Exception as e:
            return {"success": False, "data": f"An error occurred: {e}"}


    def do_list(self, arg):
        args = shlex.split(arg)
        if not args:
            return {"success": False, "data": "Error: Incomplete list command."}
        list_cmd = args[0].lower()
        try:
            if list_cmd == 'examples':
                search_path = Path(f'{self.meta_data["root_dir"]}/examples')
                if not search_path.is_dir():
                    return {"success": False, "data": f"Error: No examples found"}
                all_items = list(search_path.iterdir())
                examples = [path.name for path in all_items if path.is_dir()]
                return {"success": True, "data": examples}
            elif list_cmd == 'results': 
                if len(args) < 2:
                    return {"success": False, "data": "Error: 'list results' requires a <gid>."}
                gid = int(args[1])
                if gid<0 or gid >= len(self.jobs):
                    return {"success": False, "data": "Error: Invalid <gid>."}
                job = self.jobs[gid]
                results = {}
                with job["lock"]:
                    task_list = copy.deepcopy(job["task_list"])
                    result_dir= job["result_dir"]
                if result_dir:
                    file_path = result_dir + "/config.yaml"
                    if Path(file_path).exists():
                        results["-1"] = {"description": "" , "files": ["config.yaml"]}
                for task_id, task in enumerate(task_list):
                    params = task['params']
                    target_dir = params["target_dir"]
                    filenames = params["run_params"]["files_to_copy"]
                    valid_files = []
                    for filename in filenames:
                        file_path = f"{target_dir}/{filename}"
                        if Path(file_path).exists():
                            valid_files.append(filename)
                    if valid_files:
                        results[str(task_id)] = {"description": str(task["label"]) , "files": valid_files}
                return {"success": True, "data": results}
            elif list_cmd == 'nodes': 
                return {"success": True, "data": self.resources["nodes"]}
            else:
                return {"success": False, "data": "Error: Invalid list command. Use 'examples' or 'results <gid> [path]'"}
        except Exception as e:
            return {"success": False, "data": f"An error occurred: {e}"}

    def do_cat(self, arg):
        args = shlex.split(arg)
        if len(args) == 1:
            try:
                example = args[0]
                base_dir = Path(self.meta_data["root_dir"]) / "examples"
                safe_base_dir = base_dir.resolve()
                file_path = (base_dir / example / "config.yaml").resolve()
                if safe_base_dir not in file_path.parents:
                    return {"success": False, "data": "Error: Invalid path or traversal attempt."}
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        return {"success": True, "data": content}
                else:
                    return {"success": False, "data": "Error: Example does not exist"}
            except Exception as e:
                return {"success": False, "data": f"An error occurred while reading the example: {e}"}
        elif len(args) == 2:
            try:
                gid = int(args[0])
                task_id = int(args[1])
                job = self.jobs[gid]
                with job["lock"]:
                    task_list = job["task_list"]
                    task = copy.deepcopy(task_list[task_id])
                task["params"]["target_dir"] = ''
                task["params"]["tmpdir"] = ''
                return {"success": True, "data": task}
            except Exception as e:
                return {"success": False, "data": f"An error occurred while reading the task data: {e}"}
        elif len(args) == 3:
            try:
                gid = int(args[0])
                task_id = int(args[1])
                filename = args[2]
                job = self.jobs[gid]
                if task_id == -1:
                    with job["lock"]:
                        result_dir = job["result_dir"]
                    with open(result_dir + "/config.yaml", 'r', encoding='utf-8') as f:
                        content = f.read()
                        return {"success": True, "data": content}
                with job["lock"]:
                    task_list = job["task_list"]
                    task = task_list[task_id]
                    params = task['params']
                    target_dir = params["target_dir"]
                base_dir = Path(target_dir)
                safe_base_dir = base_dir.resolve()
                file_path = (base_dir / filename).resolve()
                if safe_base_dir not in file_path.parents:
                    return {"success": False, "data": "Error: Invalid path or traversal attempt."}
                if file_path.exists():
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                        return {"success": True, "data": content}
                else:
                    return {"success": False, "data": "Error: File does not exist"}
            except Exception as e:
                return {"success": False, "data": f"An error occurred while reading file: {e}"}
        else:
            return {"success": False, "data": "Error: Invalid cat command. Valid usage: cat <example> or cat <gid> <task_id> or cat <gid> <task_id> <filename>"}

    def do_isdone(self, arg):
        args = shlex.split(arg)
        if not args:
            return {"success": False, "data": "Error: Incomplete completion query."}
        try:
            gid = int(args[0])
            if gid<0 or gid >= len(self.jobs):
                return {"success": False, "data": "Error: Invalid <gid>."}
            job = self.jobs[gid]
            with job["lock"]:
                status = job["status"]
            return {"success": True, "data": status}
        except Exception as e:
                return {"success": False, "data": f"An error occurred: {e}"}

    def do_status(self, arg):
        if not arg:
            job_summaries = []
            for job in self.jobs:
                with job["lock"]:
                    summary = {
                        "job_id": job["gid"],
                        "status": job["status"],
                        "running_task_label": job["running_task"].get("label", "None") if job["running_task"] else "None",
                        "pending_tasks": job["job_queue"].qsize(),
                        "completed_tasks": len(job["completed_tasks"]),
                        "failed_tasks": len(job["failed_tasks"]),
                        "error": job.get("error", None)
                    }
                job_summaries.append(summary)
            return {"success": True, "data": {"job_summary": job_summaries}}
        try:
            gid = int(arg)
            if gid < 0 or gid >= len(self.jobs):
                return {"success": False, "error": f"Invalid job ID: {gid}. Max ID is {len(self.jobs) - 1}."}
            job = self.jobs[gid]
        except (ValueError, TypeError, IndexError):
            return {"success": False, "error": f"Invalid job ID: '{arg}'. Must be an integer."}
        with job["lock"]:
            running_task_data = {}
            if job["running_task"]:
                task = job["running_task"]
                running_task_data = {
                    "job_id": job["gid"],
                    "task_id": task.get("id", ''), 
                    "label": task.get("label", 'Unknown'),
                    "msg": 'Running'
                }
            else:
                running_task_data = {"job_id": job["gid"], "task_id": -1, "label": '', "msg": 'No task running'}
            max_task_id = running_task_data["task_id"]
            completed_tasks = []
            for task in job["completed_tasks"]:
                completed_tasks.append({
                    "job_id": job["gid"],
                    "task_id": task.get("id", ''),
                    "label": task.get("label", 'Unknown'),
                    "msg": ''
                })
                if task.get("id", -1) > max_task_id:
                    max_task_id = task.get("id", -1)
            failed_tasks = []
            job_error = job.get('error', 'Unknown')
            for task in job["failed_tasks"]:
                failed_tasks.append({
                    "job_id": job["gid"],
                    "task_id": task.get("id", ''),
                    "label": task.get("label", 'Unknown'),
                    "msg": f"Error: {job_error}"
                })
                if task.get("id", -1) > max_task_id:
                    max_task_id = task.get("id", -1)
            pending_tasks = []
            for index, task in enumerate(job["task_list"]):
                if index > max_task_id :
                    pending_tasks.append({
                        "job_id": job["gid"],
                        "task_id": index,
                        "label": task.get("label", 'Unknown'),
                        "msg": ''
                    })
            report_data = {
                "running": running_task_data,
                "pending": pending_tasks,
                "completed": completed_tasks,
                "failed": failed_tasks,
                "job_status": job["status"], 
                "job_error": job.get("error", None) 
            }
        return {"success": True, "data": report_data}

        
    def shutdown_all(self):
        
        return

    def do_exit(self, arg):
        self.shutdown_all()
        return {"success": True, "data": ''}

    def do_quit(self, arg):
        return self.do_exit(arg)

    def do_EOF(self, arg):
        return self.do_exit(arg)

    def emptyline(self):
        pass

    def execute_command(self, command_line):
        try:
            command_line = command_line.strip()
            if not command_line:
                f = StringIO()
                with redirect_stdout(f):
                    self.emptyline()
                return f.getvalue()
            parts = command_line.split(None, 1)
            command = parts[0]
            arg = parts[1] if len(parts) > 1 else ""
            method_name = f'do_{command}'
            method = getattr(self, method_name, None)
            if method:
                f = StringIO()
                with redirect_stdout(f):
                    ret = method(arg) 
                stdout_value = f.getvalue()
                if ret["data"] == "stdout":
                    ret["data"] = stdout_value
                    stdout_value = ''
                return {"return": ret, "stdout": stdout_value} 
            else:
                return f"Error: Unknown command '{command}'"
        except Exception as e:
            return f"Error executing command '{command_line}': {e}"


if __name__ == '__main__':
    shell = ColdpressShell()
    class ToolRequestHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data)
                command_line = data.get('command')
                if not command_line:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Missing 'command' field"}).encode())
                    return
                output = shell.execute_command(command_line)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"response": output}).encode())
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        def log_message(self, format, *args):
            pass

    PORT = 50000
    server_address = ('', PORT)
    httpd = HTTPServer(server_address, ToolRequestHandler)
    print(f"Starting Coldpress API server on port {PORT}...")
    print("Send POST requests to http://127.0.0.1:50000/ with JSON body: {\"command\": \"...\"}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, shutting down...")
        shell.do_exit(None)
        httpd.server_close()
        print("Server shut down.")
        sys.exit(0)
