import io
import os
import sys
import copy
import yaml
import json
import time
import shlex
import queue
import shutil
import threading
import contextlib
from io import StringIO
from pathlib import Path
from datetime import datetime
from kubernetes import client, config
from concurrent.futures import wait
from contextlib import redirect_stdout
from concurrent.futures import ThreadPoolExecutor
from openshift_runtime import runtime
from http.server import BaseHTTPRequestHandler, HTTPServer
from models import ConfigFile
from pydantic import ValidationError
from parsers import BenchmarkParser, vLLMParser, DiscoveryParser


def get_root_dir():
    ROOT_VAR_NAME = "COLDPRESS_ROOT_DIR"
    root_dir = os.getenv(ROOT_VAR_NAME)
    if not root_dir:
        return os.path.normpath(os.getcwd())
    if not os.path.isabs(root_dir):
        print(
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
        self.log = []
        self.meta_data = {
            "timestamp": datetime.now().strftime("%Y_%m_%d_%H_%M_%S"),
            "tmpdir": "",
            "root_dir": get_root_dir(),
        }
        self.meta_data["tmpdir"] = (
            f"/tmp/coldpress_tmpdir_{self.meta_data['timestamp']}"
        )
        os.makedirs(self.meta_data["tmpdir"], exist_ok=False)
        self.parsers = {
            "vllm": vLLMParser(),
            "benchmark": BenchmarkParser(),
            "discovery": DiscoveryParser(),
        }
        self.runtime = runtime()
        self.resources = {"nodes": self.runtime.get_nodes()}
        # Job Management
        self.jobs = {}  # Dict[int, Dict]
        self.jobs_lock = threading.Lock()
        # Concurrency / Runtime Interface
        self.command_queue = queue.Queue()
        self.cleanup_queue = queue.Queue()
        self.shutdown_event = threading.Event()
        self.command_executor = threading.Thread(target=self.command_worker, daemon=True)
        self.monitor_executor = threading.Thread(target=self.monitor_worker, daemon=True)
        self.cleanup_executor = threading.Thread(target=self.cleanup_worker, daemon=True)
        self.command_executor.start()
        self.monitor_executor.start()
        self.cleanup_executor.start()
        print("Coldpress Nodes:")
        print(self.resources["nodes"])

    def process_command(self, cmd_data):
        """
        Handlers for commands popped from the command_queue.
        Execute runtime operations here to offload the main shell thread.
        """
        cmd_type = cmd_data.get("type")
        gid = cmd_data.get("gid")

        if cmd_type == "launch":
            task_list = cmd_data.get("task_list")
            print(f"[Command] Launching Job {gid}...")
            try:
                # Delegate to the openshift runtime to create the JobSet
                self.runtime.run(gid, task_list)
                # Update status safely
                with self.jobs_lock:
                    if self.jobs[gid]["status"] == "pending":
                        self.jobs[gid]["status"] = "running"
            except Exception as e:
                print(f"[Command] Failed to launch Job {gid}: {e}")
                with self.jobs_lock:
                    self.jobs[gid]["status"] = "failed"
                    self.jobs[gid]["error"] = str(e)
                    # Queue for cleanup if creation failed halfway
                    self.cleanup_queue.put(gid)
        elif cmd_type == "stop":
            print(f"[Command] Stopping Job {gid}...")
            try:
                # Runtime 'delete' is the closest equivalent to stopping in this implementation
                self.cleanup_queue.put(gid)
                with self.jobs_lock:
                    self.jobs[gid]["status"] = "failed" # or "stopped"
                    self.jobs[gid]["error"] = "Stopped by user"
            except Exception as e:
                print(f"[Command] Failed to stop Job {gid}: {e}")

    def cleanup_worker(self):
        print("Cleanup worker thread started.")
        while not self.shutdown_event.is_set():
            try:
                gid = self.cleanup_queue.get(timeout=1)
                print(f"[Cleanup] Processing cleanup for Job {gid}...")
                # Perform the blocking delete/collect
                self.runtime.delete(gid)
                with self.jobs_lock:
                    if gid in self.jobs:
                        self.jobs[gid]["status"] = "completed"
                        # Mark all tasks as completed using the stored task list
                        self.jobs[gid]["completed_tasks"] = self.jobs[gid].get("task_list", [])
                print(f"[Cleanup] Job {gid} cleanup finished.")
                self.cleanup_queue.task_done()
            except queue.Empty:
                pass
                time.sleep(5)
            except Exception as e:
                print(f"[Cleanup] Error in cleanup worker: {e}")

    def command_worker(self):
        """Dedicated worker for processing user commands immediately."""
        print("Command worker started.")
        while not self.shutdown_event.is_set():
            try:
                cmd_data = self.command_queue.get(timeout=1)
                self.process_command(cmd_data) # Refactor command logic into this method
                self.command_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[Command] Error: {e}")

    def monitor_worker(self):
        """Dedicated worker for polling K8s status."""
        print("Monitor worker started.")
        while not self.shutdown_event.is_set():
            # 1. Identify active jobs quickly within lock
            active_jobs = []
            with self.jobs_lock:
                for gid, job_data in self.jobs.items():
                    if job_data["status"] in ["running", "pending"]:
                        active_jobs.append(gid)
            # 2. Poll status outside the lock (Network I/O)
            for gid in active_jobs:
                try:
                    status_info = self.runtime.status(gid)
                    state = status_info.get("state", "Unknown")
                    # 3. Update state atomically within lock
                    if state in ["Completed", "Failed"]:
                        with self.jobs_lock:
                            # Re-verify status hasn't changed by another thread
                            if self.jobs[gid]["status"] not in ["completed", "failed", "collecting"]:
                                if state == "Completed":
                                    print(f"[Monitor] Job {gid} completed. Queueing cleanup.")
                                    self.jobs[gid]["status"] = "collecting"
                                    self.cleanup_queue.put(gid)
                                elif state == "Failed":
                                    self.jobs[gid]["status"] = "failed"
                                    self.jobs[gid]["error"] = status_info.get("reason", "JobSet Failed")
                                    self.cleanup_queue.put(gid)
                except Exception as e:
                    print(f"[Monitor] Error checking Job {gid}: {e}")
            time.sleep(5)

    def log_msg(self, msg):
        now = datetime.now()
        timestamp_tag = now.strftime("%Y%m%d%H%M%S%f")
        self.log.append(f"[{timestamp_tag}] [Console] {msg}")
        print(self.log[-1])

    def create_job_entry(self):
        with self.jobs_lock:
            gid = len(self.jobs)
            self.jobs[gid] = {
                "gid": gid,
                "status": "pending",
                "task_list": [],
                "completed_tasks": [],
                "failed_tasks": [],
                "result_dir": None,
                "error": None,
                "log": [], # Simplified log
            }
        self.log_msg(f"Job {gid} created.")
        return gid

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
        except Exception:
            return {"success": False, "data": "Error: Incomplete discover command."}
        
        config_dict = {}
        if discovery_type == "network":
            # Prepare the single discovery task
            gid = self.create_job_entry()
            params = {
                "run_params": self.parsers["discovery"].parse(
                    discovery_type=discovery_type, config=config_dict
                ),
                "node_id": str(target_node),
                "node_name": self.resources["nodes"][str(target_node)]["name"],
                "tmpdir": f"{self.meta_data['tmpdir']}/{gid}/discover", 
                "target_dir": self.meta_data["root_dir"] + "/system/config/network/" + target_node,
                "tag": f"{gid}-discover",
            }
            task_list = [{
                "label": f"network discovery for Node: {params['node_id']} ({params['node_name']})",
                "params": params,
                "task_id": 0
            }]
            with self.jobs_lock:
                self.jobs[gid]["task_list"] = task_list
                self.jobs[gid]["result_dir"] = params["target_dir"]
            # Enqueue
            self.command_queue.put({
                "type": "launch",
                "gid": gid,
                "task_list": task_list
            })
            return {"success": True, "data": gid}
        else:
            return {"success": False, "data": "Error: Unsupported discovery type."}

    def do_stop(self, arg):
        try:
            gid = int(arg)
            with self.jobs_lock:
                if gid not in self.jobs:
                     return {"success": False, "data": f"Error: Invalid job ID: {gid}."}
            
            self.command_queue.put({"type": "stop", "gid": gid})
            return {"success": True, "data": f"Stop signal sent to job {gid}."}
        except ValueError:
            return {"success": False, "data": "Error: Invalid job ID."}    


    def do_launch(self, arg):
        try:
            args = shlex.split(arg)
            if not args or args[0].lower() != "example":
                return {"success": False, "data": "Error: Usage: launch example <name>"}
            example = args[1].lower()
            config_file = f"{self.meta_data['root_dir']}/examples/{example}/config.yaml"
            if not os.path.isfile(config_file):
                return {"success": False, "data": f"Error: Cannot find configuration for {example}."}
            try:
                with open(config_file, "r") as f:
                    raw_config = yaml.safe_load(f)
                    config = ConfigFile.model_validate(raw_config)
            except ValidationError as e:
                return {"success": False, "data": f"Configuration validation failed: {e}"}
            # Setup Result Directory
            result_dir_base = os.path.join(
                self.meta_data["root_dir"],
                "coldpress_results",
                f"results_{self.meta_data['timestamp']}",
            )
            gid = self.create_job_entry()
            job_result_dir = f"{result_dir_base}/{gid}"
            os.makedirs(job_result_dir, exist_ok=True)
            self.log_msg(f"Result directory: {job_result_dir}")
            with self.jobs_lock:
                self.jobs[gid]["result_dir"] = job_result_dir
            # Build Task List
            task_id = 0
            task_list = []
            # 1. Model Servers
            for server_config in config.model_server:
                framework = server_config.framework.name
                target_node = server_config.hardware.node
                combined_config = {"model_config": config.model, "server_config": server_config}
                params = {
                    "run_params": self.parsers[framework].parse(config=combined_config),
                    "node_id": str(target_node),
                    "node_name": self.resources["nodes"][str(target_node)]["name"],
                    "tmpdir": f"{self.meta_data['tmpdir']}/{gid}/{task_id}",
                    "target_dir": f"{job_result_dir}/{task_id}",
                    "tag": f"{gid}-{task_id}",
                }
                gpu = server_config.hardware.gpu
                task_list.append({
                    "label": f"{framework} server for Node: {params['node_id']} ({params['node_name']}) using GPU: {gpu}",
                    "params": params,
                    "task_id": task_id
                })
                task_id += 1
            # 2. Benchmarks
            for benchmark_id, benchmark in enumerate(config.benchmarks):
                target_node = benchmark.target_node
                # Determine IP (simplified logic from original)
                launch_node = benchmark.launch_node
                if launch_node == target_node:
                    target_nodeip = "127.0.0.1"
                else:
                    try:
                        with open(f"{self.meta_data['root_dir']}/system/config/network/{target_node}/network_discovery.yaml") as f:
                            network = yaml.safe_load(f)
                        target_nodeip = network["ip"]["data"]["gpu_0"]["inet"] # Assuming GPU 0
                    except:
                         target_nodeip = "127.0.0.1" # Fallback
                benchmark_with_ip = benchmark.model_copy(update={"target_nodeip": target_nodeip})
                params = {
                    "run_params": self.parsers["benchmark"].parse(benchmark=benchmark.name, config=benchmark_with_ip),
                    "node_id": str(launch_node),
                    "node_name": self.resources["nodes"][str(launch_node)]["name"],
                    "tmpdir": f"{self.meta_data['tmpdir']}/{gid}/{task_id}",
                    "target_dir": f"{job_result_dir}/{task_id}",
                    "tag": f"{gid}-{task_id}",
                }
                task_list.append({
                    "label": f"Benchmark {benchmark_id + 1}: {benchmark.name} run on Node: {params['node_id']} ({params['node_name']})",
                    "params": params,
                    "task_id": task_id
                })
                task_id += 1
            shutil.copy(config_file, f"{job_result_dir}/config.yaml")
            with self.jobs_lock:
                self.jobs[gid]["task_list"] = task_list
            # Enqueue Launch
            self.command_queue.put({
                "type": "launch",
                "gid": gid,
                "task_list": task_list
            })
            return {"success": True, "data": gid}
        except Exception as e:
            return {"success": False, "data": f"An error occurred: {e}"}

    def do_list(self, arg):
        args = shlex.split(arg)
        if not args:
            return {"success": False, "data": "Error: Incomplete list command."}
        list_cmd = args[0].lower()
        if list_cmd == "examples":
            search_path = Path(f"{self.meta_data['root_dir']}/examples")
            if not search_path.is_dir():
                return {"success": False, "data": "Error: No examples found"}
            return {"success": True, "data": [p.name for p in search_path.iterdir() if p.is_dir()]}
        elif list_cmd == "nodes":
            return {"success": True, "data": self.resources["nodes"]}
        elif list_cmd == "results":
            if len(args) < 2: return {"success": False, "data": "Error: Missing GID."}
            try:
                gid = int(args[1])
                with self.jobs_lock:
                    if gid < 0 or gid >= len(self.jobs):
                        return {"success": False, "data": "Invalid GID"}
                    job = self.jobs[gid]
                results = {}
                # Check config
                if job["result_dir"] and os.path.exists(f"{job['result_dir']}/config.yaml"):
                    results["-1"] = {"description": "", "files": ["config.yaml"]}
                # Check task results
                for task in job["task_list"]:
                    t_id = str(task["task_id"])
                    t_dir = task["params"]["target_dir"]
                    run_params = task["params"]["run_params"]
                    valid_files = []
                    if t_dir and os.path.exists(t_dir):
                        for f in run_params.get("files_to_copy", []):
                            if os.path.exists(f"{t_dir}/{f}"): valid_files.append(f)
                    if valid_files:
                        results[t_id] = {"description": task["label"], "files": valid_files}        
                return {"success": True, "data": results}
            except ValueError:
                return {"success": False, "data": "Invalid GID"}
        return {"success": False, "data": "Unknown list command."}

    def do_cat(self, arg):
        args = shlex.split(arg)
        if len(args) == 1:
            try:
                example = args[0]
                base_dir = Path(self.meta_data["root_dir"]) / "examples"
                safe_base_dir = base_dir.resolve()
                file_path = (base_dir / example / "config.yaml").resolve()
                if safe_base_dir not in file_path.parents:
                    return {
                        "success": False,
                        "data": "Error: Invalid path or traversal attempt.",
                    }
                if file_path.exists():
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        return {"success": True, "data": content}
                else:
                    return {"success": False, "data": "Error: Example does not exist"}
            except Exception as e:
                return {
                    "success": False,
                    "data": f"An error occurred while reading the example: {e}",
                }
        elif len(args) == 2:
            try:
                gid = int(args[0])
                task_id = int(args[1])
                with self.jobs_lock:
                    job = self.jobs[gid]
                task_list = job["task_list"]
                task = copy.deepcopy(task_list[task_id])
                task["params"]["target_dir"] = ""
                task["params"]["tmpdir"] = ""
                return {"success": True, "data": task}
            except Exception as e:
                return {
                    "success": False,
                    "data": f"An error occurred while reading the task data: {e}",
                }
        elif len(args) == 3:
            try:
                gid = int(args[0])
                task_id = int(args[1])
                filename = args[2]
                with self.jobs_lock:
                    job = self.jobs[gid]
                if task_id == -1:
                    result_dir = job["result_dir"]
                    with open(result_dir + "/config.yaml", "r", encoding="utf-8") as f:
                        content = f.read()
                        return {"success": True, "data": content}
                task_list = job["task_list"]
                task = task_list[task_id]
                params = task["params"]
                target_dir = params["target_dir"]
                base_dir = Path(target_dir)
                safe_base_dir = base_dir.resolve()
                file_path = (base_dir / filename).resolve()
                if safe_base_dir not in file_path.parents:
                    return {
                        "success": False,
                        "data": "Error: Invalid path or traversal attempt.",
                    }
                if file_path.exists():
                    with open(file_path, "r", encoding="utf-8") as f:
                        content = f.read()
                        return {"success": True, "data": content}
                else:
                    return {"success": False, "data": "Error: File does not exist"}
            except Exception as e:
                return {
                    "success": False,
                    "data": f"An error occurred while reading file: {e}",
                }
        else:
            return {
                "success": False,
                "data": "Error: Invalid cat command. Valid usage: cat <example> or cat <gid> <task_id> or cat <gid> <task_id> <filename>",
            }

    def do_isdone(self, arg):
        args = shlex.split(arg)
        if not args:
            return {"success": False, "data": "Error: Incomplete completion query."}
        try:
            gid = int(args[0])
            with self.jobs_lock:
                if gid < 0 or gid >= len(self.jobs):
                    return {"success": False, "data": "Error: Invalid <gid>."}
                job = self.jobs[gid]
            status = job["status"]
            return {"success": True, "data": status}
        except Exception as e:
            return {"success": False, "data": f"An error occurred: {e}"}


    def do_status(self, arg):
        if not arg:
            summaries = []
            with self.jobs_lock:
                for gid, job in self.jobs.items():
                    summaries.append({
                        "job_id": gid,
                        "status": job["status"],
                        "completed_tasks": len(job.get("completed_tasks", [])),
                        "total_tasks": len(job.get("task_list", [])),
                        "error": job["error"]
                    })
            return {"success": True, "data": {"job_summary": summaries}}
        else:
            try:
                gid = int(arg)
                with self.jobs_lock:
                    if gid not in self.jobs: return {"success": False, "data": "Invalid GID"}
                    job = self.jobs[gid]
                    completed_tasks = job.get("completed_tasks", [])
                    data = {
                        "job_status": job["status"],
                        "job_error": job["error"],
                        "pending/running": [task for task in job.get("task_list", []) if task not in completed_tasks], 
                        "completed": completed_tasks,
                    }
                    return {"success": True, "data": data}
            except ValueError:
                 return {"success": False, "data": "Invalid GID"}

    def shutdown_all(self):
        print("Shutting down...")
        with self.jobs_lock:
            for gid in range(len(self.jobs)):
                self.stop(str(gid))
        self.shutdown_event.set()
        self.runtime_executor.shutdown(wait=False)
        return

    def do_exit(self, arg):
        self.shutdown_all()
        return {"success": True, "data": ""}


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
            method_name = f"do_{command}"
            method = getattr(self, method_name, None)
            if method:
                f = StringIO()
                with redirect_stdout(f):
                    ret = method(arg)
                stdout_value = f.getvalue()
                if ret["data"] == "stdout":
                    ret["data"] = stdout_value
                    stdout_value = ""
                return {"return": ret, "stdout": stdout_value}
            else:
                return {
                    "return": {
                        "success": False,
                        "data": f"Error: Unknown command '{command}'",
                    },
                    "stdout": "",
                }
        except Exception as e:
            return {
                "return": {
                    "success": False,
                    "data": f"Error executing command '{command_line}': {e}",
                },
                "stdout": "",
            }


if __name__ == "__main__":
    shell = ColdpressShell()

    class ToolRequestHandler(BaseHTTPRequestHandler):
        def do_POST(self):
            try:
                content_length = int(self.headers["Content-Length"])
                post_data = self.rfile.read(content_length)
                data = json.loads(post_data)
                command_line = data.get("command")
                if not command_line:
                    self.send_response(400)
                    self.send_header("Content-type", "application/json")
                    self.end_headers()
                    self.wfile.write(
                        json.dumps({"error": "Missing 'command' field"}).encode()
                    )
                    return
                output = shell.execute_command(command_line)
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"response": output}).encode())
            except json.JSONDecodeError:
                self.send_response(400)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid JSON"}).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        def log_message(self, format, *args):
            pass

    PORT = 50000
    server_address = ("", PORT)
    httpd = HTTPServer(server_address, ToolRequestHandler)
    print(f"Starting Coldpress API server on port {PORT}...")
    print(
        'Send POST requests to http://127.0.0.1:50000/ with JSON body: {"command": "..."}'
    )
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received, shutting down...")
        shell.do_exit(None)
        httpd.server_close()
        print("Server shut down.")
        sys.exit(0)
