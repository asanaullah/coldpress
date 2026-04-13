<!-- Assisted by: Claude Sonnet 4.5 -->
# Coldpress Admin Quickstart

This guide covers cluster-wide configuration tasks that require admin privileges. These are typically done once by a cluster administrator.

**Prerequisites:**
- Admin access to Kubernetes/OpenShift cluster (tested on OpenShift 4.21.5, Kubernetes v1.34.4)
- Kueue operator installed (tested with v0.11.6, API v1beta1)
- JobSet operator installed (tested with v1.0.0, API v1alpha2)
- **`kubectl` or `oc` CLI installed and configured** (tested with oc 4.17.0, required for applying manifests)
- Coldpress CLI tools installed (see main [README.md](../README.md))

---

## Step 1: Generate and Apply Cluster Configuration

**What:** Generate manifests for cluster-wide resources (ResourceFlavors, ClusterQueue).

**Why:** Sets up the Kueue queueing system for GPU allocation across the cluster.

**Step 1a: Generate manifests**
```bash
coldpress-setup generate cluster ocp-test-nerc-mghpcc.yaml
```

This generates:
- `manifests/cluster-ocp-test-nerc-mghpcc-<timestamp>.yaml` - Kubernetes manifests
- `manifests/label-nodes-ocp-test-nerc-mghpcc.sh` - Node labeling script

**Step 1b: Review the generated manifests**
```bash
cat manifests/cluster-ocp-test-nerc-mghpcc-*.yaml
cat manifests/label-nodes-ocp-test-nerc-mghpcc.sh
```

**Step 1c: Run the labeling script**

Coldpress uses node labels for scheduling jobs to specific GPU nodes.

```bash
./manifests/label-nodes-ocp-test-nerc-mghpcc.sh
```

**Step 1d: Apply the manifest to the cluster**
```bash
oc apply -f manifests/cluster-ocp-test-nerc-mghpcc-*.yaml
```

**What gets created:**
- **Node labels** (via labeling script in Step 1c): `coldpress.node: 0, 1, etc.`
- **ResourceFlavors** (via manifest in Step 1d): GPU node pools referencing labeled nodes
- **ClusterQueue** (via manifest in Step 1d): cluster-queue-coldpress

**Note:** The Kueue and JobSet operators must already be installed on the cluster. This step only creates the Kueue custom resources that use those operators.

**Verification:**
```bash
# Verify node labels were applied
oc get nodes --show-labels | grep coldpress.node

# Verify cluster resources were created
oc get clusterqueues
oc get resourceflavors
```

**You should see:**
```
# Node labels
wrk-4   ... coldpress.node=0 ...
wrk-6   ... coldpress.node=1 ...

# Cluster resources
NAME                      AGE
cluster-queue-coldpress   5s

NAME    AGE
node0   5s
node1   5s
```

**About node labeling:**

The cluster configuration's `nodes` section specifies which nodes should be labeled with coldpress IDs. Node IDs are automatically assigned based on the order in the YAML file (0, 1, 2, ...). The generated labeling script applies these labels, which are required for coldpress to schedule jobs to specific GPU nodes.

---

## Step 2: Generate and Apply Project Configuration

**What:** Generate manifests for a project namespace with storage and queueing resources.

**Why:** Provides isolated workspace for a research group or project team.

**Step 2a: Generate manifests**
```bash
coldpress-setup generate project researcher-a.yaml
```

This generates a timestamped manifest file (e.g., `manifests/project-researcher-a-20260413-152928.yaml`).

**Step 2b: Review the generated manifest**
```bash
cat manifests/project-researcher-a-*.yaml
```

Review the RBAC permissions and resource allocations before applying.

**Step 2c: Apply the manifest to the cluster**
```bash
oc apply -f manifests/project-researcher-a-*.yaml
```

**This creates:**
- Namespace: `researcher-a`
- LocalQueue: `local-queue-researcher-a` (connects to ClusterQueue)
- PersistentVolumeClaim: `researcher-a-storage` (500Gi)
- RBAC: ServiceAccount, Role, RoleBinding for job execution

**Verification:**
```bash
oc get namespace researcher-a
oc get pvc -n researcher-a
oc get localqueues -n researcher-a
```

**You should see:**
```
NAME           STATUS   AGE
researcher-a   Active   5s

NAME                    STATUS   VOLUME   CAPACITY   ACCESS MODES   AGE
researcher-a-storage    Bound    pvc-...  500Gi      RWX            5s

NAME                        CLUSTERQUEUE              AGE
local-queue-researcher-a    cluster-queue-coldpress   5s
```

---

## Step 3: Generate and Apply User RBAC

**What:** Generate manifests to grant an existing cluster user permission to submit jobs to the project namespace.

**Why:** Allows regular users to create and manage JobSets without admin privileges.

**Prerequisites:**
- User must already exist in the cluster's authentication system (OpenShift OAuth, LDAP, etc.)
- Project configuration must be applied first (creates the Role that this RoleBinding references)

**Step 3a: Generate manifests**
```bash
coldpress-setup generate user coldpress-user.yaml
```

This generates a timestamped manifest file (e.g., `manifests/user-coldpress-user-20260413-153047.yaml`).

**User config example** (`users/coldpress-user.yaml`):
```yaml
username: coldpress-user
namespaces:
  - researcher-a
```

**Step 3b: Review the generated manifest**
```bash
cat manifests/user-coldpress-user-*.yaml
```

**Step 3c: Apply the manifest to the cluster**
```bash
oc apply -f manifests/user-coldpress-user-*.yaml
```

**This creates:**
- RoleBinding: `coldpress-user-coldpress-user` in namespace `researcher-a`
- Binds existing user to existing Role: `coldpress-user-role` (created by project setup)
- Grants permissions: create/manage JobSets, view Jobs/Pods/Services

**Important:** This does NOT create a user account. Users must already exist in your cluster's authentication system.

**Verification:**
```bash
oc get rolebindings -n researcher-a | grep coldpress-user
```

**Expected output:**
```
coldpress-user-coldpress-user   5s
```

---

## Summary

You have now completed the admin setup for Coldpress:

1. ✓ Applied node labels (required for job scheduling)
2. ✓ Configured cluster-wide Kueue resources (ClusterQueue, ResourceFlavors)
3. ✓ Set up project namespace with storage and queueing (LocalQueue, PVC)
4. ✓ Configured user RBAC for job submission

**Next steps:**

Users can now follow the [User Quickstart Guide](quickstart_user.md) to submit and manage AI/HPC workloads.

**For additional users:**
1. Create a user config in `users/username.yaml`
2. Apply with `coldpress-setup generate user username.yaml`
3. Apply the manifest with `oc apply -f manifests/user-*.yaml`

**For additional projects:**
1. Create a project config in `projects/project-name.yaml`
2. Apply with `coldpress-setup generate project project-name.yaml`
3. Apply the manifest with `oc apply -f manifests/project-*.yaml`
