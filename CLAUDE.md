# Coldpress Project Documentation

## What is Coldpress?

Coldpress is a tool for AI/HPC workload orchestration on Kubernetes/OpenShift. It takes vanilla Kubernetes Job specifications and an intent file, then generates JobSet, Kubeflow (PyTorchJob, TFJob, MPIJob), or KServe (InferenceService) manifests with automatic configuration, discovery job injection, and result handling.

## Architecture

- **Input**: Vanilla K8s Job spec (job-spec.yaml) + Intent file (intent.yaml)
- **Processing**: Transform and inject configuration based on project settings
- **Output**: Backend-specific manifests (JobSet/Kubeflow/KServe) + helper scripts + discovery job injection
- **Features**:
  - Discovery job injection to capture system state
  - Automatic result handling and storage
  - Prescriptive manifest generation with templating
  - Helper scripts (run.sh, monitor.sh, explore.sh, cleanup.sh)

## Critical Value Proposition Questions

**This project needs clear answers to the following questions before it can justify adoption:**

### 1. Core Value Questions

- **Who asked for this?** What pain point drove the creation of coldpress?
- **Can you name 3 people who would choose this over Kubeflow + Helm?**
- **What's the 10-second pitch for why someone should adopt coldpress?**
- **What problem does coldpress solve that existing tools (Kubeflow, Helm, Kustomize) don't?**

### 2. Abstraction Overhead

- **Why write a 76-line K8s Job + 20-line intent when you could write a PyTorchJob directly?**
- **What's the productivity gain that justifies the learning curve?**
- **Does the intent.yaml abstraction reduce complexity or just move it?**
- **How does coldpress reduce time-to-first-job compared to standard Kubeflow?**

### 3. Competing with Proven Tools

- **Kubeflow already handles distributed training** - what does coldpress add?
- **Helm/Kustomize handle templating and configuration** - why a new format?
- **Standard K8s workflows are well-documented** - why create custom formats?
- **Lock-in risk**: What happens when users outgrow coldpress or need features it doesn't support?

### 4. Target User Clarity

- **ML researchers want simpler, not different YAML** - does coldpress simplify or complicate?
- **Platform teams want standard tools, not custom abstractions** - what's the platform team value?
- **Who is the primary user?** Data scientists? ML engineers? Platform engineers? Research teams?
- **What's their skill level assumption?** K8s experts? K8s beginners? ML practitioners with no K8s experience?

### 5. Discovery Job Injection

- **What problem does discovery job injection solve?**
- **Why capture system state for each job?** What's the use case?
- **How is this better than standard logging/monitoring tools?**
- **What do users do with the captured state?**
- **Does this add meaningful value or just create data overhead?**
- **How does discovery integrate with existing observability stacks?**
- **Can users achieve the same thing with Kubernetes audit logs or pod lifecycle hooks?**

### 6. Automatic Result Handling

- **What does "automatic result handling" mean specifically?**
- **How is this better than standard PVC + user-defined output paths?**
- **Does coldpress enforce a specific output structure?** Is that helpful or constraining?
- **How does this integrate with MLOps tools (MLflow, Weights & Biases, etc.)?**
- **What happens when users need custom result processing?**
- **Can users easily move results out of the coldpress ecosystem?**

### 7. Prescriptive Manifest Generation

- **What does "prescriptive" mean in this context?**
- **Who defines the prescriptions?** Platform team? Coldpress maintainers? Users?
- **How does prescriptive generation differ from templating (Helm/Kustomize)?**
- **Does "prescriptive" mean opinionated defaults?** What are those opinions based on?
- **Can users override prescriptions?** How much flexibility exists?
- **Does this reduce debugging surface or hide critical configuration?**
- **What's the escape hatch when prescriptions don't fit the use case?**

### 8. Integration and Ecosystem

- **How does coldpress fit into existing CI/CD pipelines?**
- **Can it coexist with existing Kubeflow/Argo workflows?**
- **What's the migration path from standard K8s/Kubeflow to coldpress?**
- **What's the migration path OUT of coldpress if needed?**

### 9. Maintenance and Sustainability

- **Who maintains coldpress long-term?**
- **How does it stay current with Kubernetes/Kubeflow API changes?**
- **What's the support model?**
- **Is this a research project or production-ready?**

## Success Metrics

Before investing further development, define:

- **Adoption metric**: How many teams/users would validate success?
- **Time-to-value**: How much time should coldpress save per job vs. standard tools?
- **Complexity reduction**: Measurable reduction in YAML lines? Configuration errors?
- **User satisfaction**: What would make a user recommend coldpress?

## Recommended Next Steps

1. **Interview potential users** - validate the problem exists and coldpress solves it
2. **Competitive analysis** - document specific scenarios where coldpress wins vs. alternatives
3. **Create comparison demos** - side-by-side: coldpress vs. raw Kubeflow vs. Helm-templated
4. **Define the niche** - if coldpress isn't for everyone, who is it specifically for?
5. **Simplify or specialize** - either make it radically simpler OR focus on a specific use case where it's clearly superior

## Notes for Contributors

- All code changes should align with a clear answer to at least one of the value questions above
- Features should be justified by user research, not technical possibility
- Complexity should only be added when it measurably reduces user burden
- Standard K8s/Kubeflow patterns should be preferred unless coldpress provides clear improvement
