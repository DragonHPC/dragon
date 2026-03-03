Dragon Policy
=============

Dragon supports the idea of a Policy that can be used to help define the placement of a Process within the Dragon
allocation, as well as "ownership" of resources such as GPUs, CPUs, etc, by that process. The use and evaluation of
Dragon policies must be consistent across the multiple ways of starting a process (Dragon Native Process, Python
Multiprocessing Process, Process Group, etc). This document attempts to layout the rules for how policies are
evaluated and applied.

Note: Policies are captured and evaluated at the time of object construction. This is a change for dragon Native
processes and Python multiprocessing Processes. In those cases, policy evaluation was done when the Process
object was 'started' as compared to when the process object was constructed.

Policy Attributes
-----------------

- placement - Where should the resource being created be placed (LOCAL, ANYWHERE, HOST_NAME, HOST_ID, DEFAULT)
- host_name - The specific hostname where the resource should be created if Placement.HOST_NAME is used
- host_id - The specific hostname where the resource should be created if Placement.HOST_ID is used
- distribution - How should resources be distributed across the available dragon allocation (ROUNDROBIN, BLOCK, DEFAULT)
- cpu_affinity - A list of CPU device IDs or cores that the object should be granted use of if available.
- gpu_env_str - To be used with gpu_affinity for vendor specific environment vars
- gpu_affinity - A list of GPU device IDs or cores that the object should be granted use of if available.
- wait_mode - For future use. Not currently implemented.
- refcounted - For future use. Not currently implemented.

Policy Hierarchy
----------------
There are multiple ways and places that you can set a policy. Dragon has defined the following hierarchy and will
attempt to merge multiple policies in priority order (with 1 being the highest priority and higher numbers being a
lower priority) to create a single combined policy for any given object:

1. An explicit policy passed to the Process / Process Group constructor or Global Services create API
2. If creating a ProcessGroup, the process group's policy will be merged with the policy (if any) of each
   ProcessTemplate
3. Use of the Python based Dragon Policy context manager.
4. The Dragon Global Policy

The Dragon Global Policy
------------------------

The Dragon `global_policy` object defines the default policy values to use when not otherwise set. The `global_policy`
object will always be merged with any user supplied Policy before evaluating the policy to ensure that the resultant
Policy object is complete and valid.

```
GS_DEFAULT_POLICY = Policy(
    placement=Policy.Placement.ANYWHERE,
    host_name="",
    host_id=-1,
    distribution=Policy.Distribution.ROUNDROBIN,
    cpu_affinity=[],
    gpu_env_str="",
    gpu_affinity=[],
    wait_mode=Policy.WaitMode.IDLE,
    refcounted=True,
)
```

Dragon currently does not have a way to modify or change the `global_policy` object.

Examples
========

No Policy
---------

In the case that no policy is passed, the object being created will use the Dragon default `global_policy`.

```
  from dragon.native import Process

  process = Process(
    target=cmdline,
  )
```

Single Explicit policy
----------------------

Any user supplied policy passed to the object's constructor, or GS Create API, will first be merged with the
Dragon `global_policy` object. The resulting merged Policy is what will be used by the object being created.

```
  from dragon.infrastructure.policy import Policy
  from dragon.native import Process

  policy = Policy(Placement=Policy.Placement.LOCAL)
  process = Process(
    target=cmdline,
    policy=policy,
  )
```

The resulting Policy for the above example will be

```
Policy(
  placement=Policy.Placement.LOCAL,              # from policy passed to object constructor.
  distribution=Policy.Distribution.ROUNDROBIN,   # from Dragon's global_policy.
  wait_mode=Policy.WaitMode.IDLE,                # from Dragon's global_policy. Not currently used.
  refcounted=True,                               # from Dragon's global_policy. Not currently used.
  ...
)
```

Using Policy Context Manager
----------------------------

The Dragon Python runtime provides a helpful Policy Context manager that can be used to establish a thread-
local stack of Policy objects. Any Process or ProcessGroup object that is created within this context will
inherit the Policy defined by the nested stack of Policy objects.

```
  from dragon.infrastructure.policy import Policy

  with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
    proc = mp.Process(
      target=cmdline,
    )
    proc.start()
```

In the above case, the Policy context manager is used to help place a Python Multiprocessing Process object (which
otherwise does not accept a policy parameter) within the Dragon allocation on a specific host.

```
  from dragon.infrastructure.policy import Policy
  from dragon.native import Process

  with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
    policy = Policy(gpu_affinity=GPU_AFFINITY)
    process = Process(
      target=cmdline,
      policy=policy,
    )
```

In this example, the policy object passed to the Process object constructor and the policy
created via the Policy context manager will be merged. The resultant policy will be:

```
Policy(
  placement = Policy.Placement.HOST_NAME,        # from Policy Context Manager.
  hostname = socket.gethostname(),               # from Policy Context Manager.
  distribution=Policy.Distribution.ROUNDROBIN,   # from Dragon Global Policy.
  gpu_affinity = GPU_AFFINITY                    # from explicit policy passed to the Process object.
  wait_mode=Policy.WaitMode.IDLE,                # from Dragon Global Policy. Not currently used.
  refcounted=True,                               # from Dragon Global Policy. Not currently used.
)
```

In a case where there are multiple, nested Policy context managers, the policy of each context manager
will be merged together (from inner-most to outer-most) before being merged with any policy passed to the
object being created.

```
  from dragon.infrastructure.policy import Policy
  from dragon.native import Process

  with Policy(placement=Policy.Placement.ANYWHERE, distribution=Policy.Distribution.BLOCK):
    with Policy(placement=Policy.Placement.HOST_NAME, host_name=socket.gethostname()):
      policy = Policy(gpu_affinity=GPU_AFFINITY)
      process = Process(
        target=cmdline,
        policy=policy,
```

In this case, the resultant policy will be:

```
Policy(
  distribution=Policy.Distribution.BLOCK,  # from outer-most context manager.
  placement=Policy.Placement.HOST_NAME,    # from inner-most context manager.
  host_name=socket.gethostname(),          # from inner-most context manager.
  gpu_affinity=GPU_AFFINITY,               # from explicit policy passed to the constructor.
  wait_mode=Policy.WaitMode.IDLE,          # from the Dragon global policy. Not currently used.
)
```

Starting a ProcessGroup
=======================

When creating a process group, a policy can be added on both the process group and process templates. The
dragon Default policy will first be merged with the process group policy. The resultant policy will then be
merged with each process template's policy.

```
from dragon.native.process_group import ProcessGroup
from dragon.native.process import MSG_PIPE, MSG_DEVNULL, Process, ProcessTemplate

group_policy = Policy(distribution=Policy.Distribution.BLOCK)
grp = ProcessGroup(restart=False, pmi_enabled=True, polic=group_policy)

template_policy_1 = Policy(gpu_affinity=GPU_AFFINITY_1)

# Pipe the stdout output from the head process to a Dragon connection
grp.add_process(
  nproc=1,
  template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_PIPE, policy=template_policy_1)
)

template_policy_2 = Policy(gpu_affinity=GPU_AFFINITY_2)

# All other ranks should have their output go to DEVNULL
grp.add_process(
  nproc=num_ranks-1,
  template=ProcessTemplate(target=exe, args=args, cwd=run_dir, stdout=MSG_DEVNULL, policy=template_policy_2)
)
```

The first process template will have a policy matching

```
Policy(
  placement = Policy.Placement.ANYWHERE,         # from Dragon Global Policy
  distribution=Policy.Distribution.BLOCK,        # from Group policy
  gpu_affinity = GPU_AFFINITY_1                  # from teplate_policy_1
  wait_mode=Policy.WaitMode.IDLE,                # from Dragon Global Policy. Not currently used.
  refcounted=True,                               # from Dragon Global Policy. Not currently used.
)
```

The rest of the process templates will have a policy matching

```
Policy(
  placement = Policy.Placement.ANYWHERE,         # from Dragon Global Policy
  distribution=Policy.Distribution.BLOCK,        # from Group policy
  gpu_affinity = GPU_AFFINITY_2                  # from teplate_policy_2
  wait_mode=Policy.WaitMode.IDLE,                # from Dragon Global Policy. Not currently used.
  refcounted=True,                               # from Dragon Global Policy. Not currently used.
)
```
