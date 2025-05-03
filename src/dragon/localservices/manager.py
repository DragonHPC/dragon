import asyncio
import enum
import io
import sys
import dragon.infrastructure.messages as dmsg
import dragon.infrastructure.facts as dfacts

INVALID_COMMAND = 42


class Process:
    """
    This class provides facilities for managing information
    about a managed process.
    """

    class State(enum.Enum):
        INIT = enum.auto()
        RUN = enum.auto()
        COMPLETE = enum.auto()
        FAIL = enum.auto()

    class StdInState(enum.Enum):
        # is_open below means it is open to writing, but may not yet
        # be connected to its pipe. When ProcessState is run and
        # there is data to write, then the pipe is connected.
        IS_OPEN = enum.auto()
        CLOSE_CALLED = enum.auto()
        CLOSING = enum.auto()
        CLOSED = enum.auto()

    def __init__(
        self,
        p_uid: int,
        tag_ref: int,
        cmd: str,
        args: tuple,
        env: dict = None,
        cwd: str = None,
        resp_queue=None,
        stdout_queue=None,
        stderr_queue=None,
        term_queue=None,
    ):

        # helps for debugging to have id in process. This is user-assigned id.
        self.p_uid = p_uid
        # the tag/ref is used by GS to correlate between create and create response
        # messages. It comes in on the create message as a tag field. It is sent
        # back as the ref field in the create response.
        self.tag_ref = tag_ref
        self.cmd = cmd
        self.args = args
        self.env = env
        self.cwd = cwd
        self.output_queue = {ProcessManager.TaskType.STDOUT: stdout_queue, ProcessManager.TaskType.STDERR: stderr_queue}
        self.term_queue = term_queue
        self._kill_timer = None
        self._signal_tag_ref = 0

        # If this is not None then it is the response queue to send a response message
        # to once creation is complete.
        self.resp_queue = resp_queue

        # The keys stdout and stderr are used to select the correct stream.
        self.stream = {}

        self.stdin_buf = bytearray()
        self.state = Process.State.INIT
        self.pid = None
        self._stat = None

        self.stdin_state = self.StdInState.IS_OPEN
        self.close_pending = False

        # keep track of the stream tasks responsible for reading
        # output so we can clean up at the end.
        self.stream_task = {}

    def __str__(self):
        rv = f"""
   Process p_uid = {self.p_uid}
     state = {self.state}
     pid = {self.pid}
     tag_ref = {self.tag_ref}
     cmd = {self.cmd}
     args = {self.args}
     env = {self.env}
     cwd = {self.cwd}
     output_queues = {self.output_queue}
     term_queue = {self.term_queue}
     kill_timer = {self._kill_timer}
     signal_tag_ref = {self._signal_tag_ref}
     resp_queue = {self.resp_queue}
     stdin_buffer = {self.stdin_buf}
     stdin_state = {self.stdin_state}
     Posix stat = {self._stat}
     close_pending = {self.close_pending}
"""
        return rv

    @property
    def signal_tag_ref(self):
        return self._signal_tag_ref

    def write_stdin(self, data: str) -> None:
        """
        Given a bytearray, write it to standard input. Returns None.
        """
        if not isinstance(data, str):
            raise ValueError(f"Only string objects can be written to stdin: {data}")

        if self.stdin_state != self.StdInState.IS_OPEN:
            raise io.UnsupportedOperation(f"stdin of subprocess is not open. State: {self.stdin_state!s}")

        byte_data = data.encode()

        if self.stdin_buf is not None:
            self.stdin_buf.extend(byte_data)
        else:
            self.stdin_buf = byte_data

    def close_stdin(self) -> None:
        """
        Close the stdin stream.
        """
        self.stdin_state = self.StdInState.CLOSE_CALLED

    def stop(self):
        """
        Send the process SIGTERM.
        """
        return self._stat.terminate()

    def signal(self, sig, tag_ref):
        """
        Send the process a signal.
        """
        self._signal_tag_ref = tag_ref
        return self._stat.send_signal(sig)

    def set_kill_timer(self, timer_task):
        self._kill_timer = timer_task

    def clear_kill_timer(self):
        self._kill_timer = None

    def cancel_kill_timer(self, logger):
        if self._kill_timer is not None:
            logger.info(f"Attempting to cancel kill timer for process {self.p_uid}")
            self._kill_timer.cancel()
            logger.info(f"Cancel Success for process {self.p_uid}")
            self._kill_timer = None


class ProcessManager:
    """
    ProcessManager

    This class provides asynchronous methods for starting up
    processes on the local node.  It forwards stdout/stderr
    and redirects stdin to processes. It is also the driver
    of asynchronous execution of system tasks provided by the
    shepherd. The ProcessManager and the Shepherd work together
    to provide the services of the Shepherd component in Dragon.
    """

    class TaskType(enum.Enum):
        """
        The TaskType is a identifier of the type of task being executed asynchronously
        in the ProcessManager and also identifies the type of stream.
        """

        PROCESS = -2  # This indicates a user process task.
        SYS = -1  # This indicates a system task.
        # The stdin, stdout, and stderr values should remain
        # 0,1, and 2 respectively. They coincide with Posix
        # file descriptors but also with the FileDescriptor
        # enum in messages.
        STDIN = 0
        STDOUT = 1
        STDERR = 2

    def __init__(self, shepherd):

        self.processes = {}  # This is a dictionary of running/active processes.
        self.tasks = []
        self.new_processes = []  # This is for new processes that are to be created.
        self.task_tbl = {}
        self.shepherd = shepherd

        self.ect = asyncio.get_event_loop().create_task

    def __str__(self):

        tasks = "\n".join([str(t) for t in self.tasks])

        procs = "\n".join([str(p) for p in self.processes])
        new_procs = "\n".join([str(p) for p in self.new_processes])

        rv = f"""
    Process Manager
Tasks:
{tasks}

Task Names Reverse Map:
{self.task_tbl.values()!s}

Processes:
{procs}

New Processes:
{new_procs}"""
        return rv

    def create_task(self, fun, task_type, id_num):
        task_name = f"{task_type!s}:{id_num=!s}"
        self.task_tbl[(task := self.ect(fun, name=task_name))] = (task_type, id_num)
        return task

    def _handle_started_procs(self, finished):

        local_new_tasks = []

        for task in set(finished):
            task_type, p_uid = self.task_tbl[task]
            if task_type == self.TaskType.PROCESS:
                # This check is necessary because it is possible that a task that finishes might
                # refer to a process that has already finished and if so, we don't want to
                # get an exception trying to find it in the process table.
                if p_uid in self.processes:
                    process = self.processes[p_uid]

                    if process.state == Process.State.INIT:
                        process.state = Process.State.RUN

                        process.stream[self.TaskType.STDOUT] = process._stat.stdout
                        process.stream[self.TaskType.STDERR] = process._stat.stderr
                        process.stream[self.TaskType.STDIN] = process._stat.stdin

                        proc_task = self.create_task(self.async_app_done(p_uid), self.TaskType.PROCESS, p_uid)

                        stdout_task = self.create_task(
                            self.async_read_output(p_uid, self.TaskType.STDOUT), self.TaskType.STDOUT, p_uid
                        )

                        stderr_task = self.create_task(
                            self.async_read_output(p_uid, self.TaskType.STDERR), self.TaskType.STDERR, p_uid
                        )

                        process.stream_task[self.TaskType.STDOUT] = stdout_task
                        process.stream_task[self.TaskType.STDERR] = stderr_task
                        local_new_tasks.extend([proc_task, stdout_task, stderr_task])
                    else:
                        if process.state == Process.State.FAIL:
                            # remove it from the process table.
                            self.app_cleanup(process.p_uid)

                # remove it from the set of finished tasks
                del self.task_tbl[task]  # ??
                finished.remove(task)

        return local_new_tasks

    def _handle_done_procs(self, finished):
        local_new_tasks = []
        theTasks = set(finished)

        for task in theTasks:
            task_type, p_uid = self.task_tbl[task]
            if task_type == self.TaskType.PROCESS:
                if p_uid in self.processes and self.processes[p_uid].state != Process.State.COMPLETE:
                    process = self.processes[p_uid]
                    self.shepherd.log.warning(f"Process {process.p_uid} finished but was not marked complete.")

                del self.task_tbl[task]
                finished.remove(task)

        return local_new_tasks

    def _handle_output(self, finished):
        local_new_tasks = []
        theTasks = set(finished)

        for task in theTasks:
            stream_type, p_uid = self.task_tbl[task]
            if stream_type in frozenset([ProcessManager.TaskType.STDOUT, ProcessManager.TaskType.STDERR]):
                if p_uid in self.processes:
                    # If p_uid is not in self._processes it is because the process already ended and
                    # this finished task had yet to be processed. There is no process so the
                    # output is ignored but likely there is no output.
                    process = self.processes[p_uid]
                    # register another read output task
                    read_task = self.create_task(self.async_read_output(p_uid, stream_type), stream_type, p_uid)
                    process.stream_task[stream_type] = read_task
                    local_new_tasks.append(read_task)

                # remove it from the set of finished tasks
                del self.task_tbl[task]
                finished.remove(task)

        return local_new_tasks

    def _handle_new_stdin_tasks(self):
        local_new_tasks = []
        for p_uid, process in self.processes.items():
            # For each process look to see if something is waiting to be written
            # to stdin. If so and there is not a current outstanding task that
            # is waiting to write to stdin already (and the stream is not closing)
            # then create a new task to write to stdin. Don't empty the buffer here.
            # The buffer will be emptied when the task runs that will actually write
            # it to stdin.
            if process.state != Process.State.INIT:
                buf = process.stdin_buf
                if buf is not None and len(buf) > 0:
                    # We don't look at state here because it will be at least open and
                    # possibly closing. But, if there is data to write, then we write it
                    # before closing.
                    if self.TaskType.STDIN not in process.stream_task:
                        stdin_task = self.create_task(self.async_write_input(p_uid), self.TaskType.STDIN, p_uid)
                        process.stream_task[self.TaskType.STDIN] = stdin_task
                        local_new_tasks.append(stdin_task)
                elif process.stdin_state == Process.StdInState.CLOSE_CALLED:
                    process.stdin_state = Process.StdInState.CLOSING
                    stdin_task = self.create_task(self.async_close_stdin(p_uid), self.TaskType.STDIN, p_uid)
                    process.stream_task[self.TaskType.STDIN] = stdin_task
                    local_new_tasks.append(stdin_task)

        return local_new_tasks

    def _handle_finished_stdin_tasks(self, finished):

        theTasks = set(finished)

        for task in theTasks:
            stream_type, p_uid = self.task_tbl[task]
            if stream_type == self.TaskType.STDIN and p_uid in self.processes:
                process = self.processes[p_uid]
                del process.stream_task[self.TaskType.STDIN]
                del self.task_tbl[task]
                finished.remove(task)

    def _handle_new_process_creation(self):
        # We create the new process tasks here outside of any other
        # running task. Otherwise, if we were to create a new task from
        # within a running task, the task loop is already running and the
        # new task then gets scheduled and run immediately. By creating
        # the new task outside of the task loop, the task will run
        # when we want it to run.
        local_new_tasks = []

        for p in self.new_processes:
            task = self.create_task(self.async_spawn_app(p), self.TaskType.PROCESS, p.p_uid)
            local_new_tasks.append(task)

        # Now reset the new process list.
        self.new_processes = []

        return local_new_tasks

    async def async_spawn_app(self, p: Process) -> bool:
        """
        Async event to start cmd with arguments args and identifier p_uid.
        """
        try:
            # check to make sure there really is a process with this p_uid.
            if p.p_uid not in self.processes:
                self.shepherd.log.warning(f"No managed process with p_uid={p.p_uid} in async_spawn_app.")
                raise ValueError(f"No managed process with p_uid={p.p_uid} in async_spawn_app.")

            self.shepherd.log.info(f"Starting Process: {p.cmd!s} {p.args!s}")
            p._stat = await asyncio.create_subprocess_exec(
                p.cmd,
                *p.args,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                stdin=asyncio.subprocess.PIPE,
                env=p.env,
                cwd=p.cwd,
            )

            self.shepherd.log.info("Process Started....")

            p.pid = p._stat.pid

            # If notification needs to be sent with the PID then send it.
            if p.resp_queue is not None:
                self.shepherd.log.info("SENDING SHProcessCreateResponse")
                p.resp_queue.send(
                    dmsg.SHProcessCreateResponse(
                        tag=0, ref=p.tag_ref, err=dmsg.SHProcessCreateResponse.Errors.SUCCESS
                    ).serialize()
                )

        except Exception as ex:
            try:
                p.state = Process.State.FAIL
                p.resp_queue.send(
                    dmsg.SHProcessCreateResponse(
                        tag=0, ref=p.tag_ref, err=dmsg.SHProcessCreateResponse.Errors.FAIL, err_info=str(ex)
                    ).serialize()
                )
                self.shepherd.log.info("Sent Failed ProcessCreateResponse")
            except:
                try:
                    self.shepherd.log.warning("Unable to send Failed ProcessCreateResponse")
                except:
                    pass

                self.shepherd.abend(sys.exc_info())
                raise ex

        return True

    async def async_app_done(self, p_uid: int) -> None:
        """
        Async event that completes if the managed application with identifier id has completed.
        """
        # check if we even have this identifier
        if p_uid not in self.processes:
            raise ValueError(f"no managed process with p_uid={p_uid}")

        process = self.processes[p_uid]
        await process._stat.wait()
        self.shepherd.log.info("In async_app_done: Process has exited.")
        self.shepherd.log.info(f"return code {process._stat.returncode!s}")

        # Call to cancel any timer set to go off for terminating the process. If no
        # timer exists, it won't hurt anything to call this.
        process.cancel_kill_timer(self.shepherd.log)

        if process.state == Process.State.RUN:
            process.state = Process.State.COMPLETE
            if process.term_queue is not None:
                self.shepherd.log.info("SENDING SHProcessExit message")
                process.term_queue.send(
                    dmsg.SHProcessExit(
                        tag=0, p_uid=process.p_uid, exit_code=process._stat.returncode, creation_msg_tag=None
                    ).serialize()
                )

    async def async_read_output(self, p_uid: int, stream_type: TaskType) -> None:
        """
        Async event to read data from the managed process stream.
        """
        try:
            process = self.processes[p_uid]
            stream = process.stream[stream_type]

            if stream is not None:
                while True:
                    if stream.at_eof():
                        break

                    output = await stream.read(dfacts.MANAGED_PROCESS_MAX_OUTPUT_SIZE_PER_MESSAGE)

                    if output is None:
                        # Must be because the stream closed.
                        break

                    if len(output) <= 0:
                        # Possibly because the stream closed.
                        break

                    output_queue = process.output_queue[stream_type]

                    if output_queue is not None:
                        self.shepherd.log.info(f"SENDING SHFwdOutput: {output!s}")
                        fwd_output_msg = dmsg.SHFwdOutput(
                            tag=0,
                            idx=self.shepherd.node_index,
                            p_uid=process.p_uid,
                            fd_num=stream_type.value,
                            data=output.decode(),
                        )
                        output_queue.send(fwd_output_msg.serialize())

        except Exception as ex:
            self.shepherd.abend(sys.exc_info())
            raise ex

    async def async_write_input(self, p_uid: int) -> None:
        """
        Async event to write data to the managed process stdin.
        """
        try:
            process = self.processes[p_uid]
            stream = process.stream[self.TaskType.STDIN]
            buf = process.stdin_buf
            # reset the buffer so if while we await below, more
            # data is written, we get that the next time.
            process.stdin_buf = bytearray()
            self.shepherd.log.info("In async_write_input")
            if not stream.is_closing():
                self.shepherd.log.info(f"Now writing input {buf!s}")
                stream.write(buf)
                await stream.drain()
                self.shepherd.log.info("Done writing")
        except Exception as ex:
            self.shepherd.abend(sys.exc_info())
            raise ex

    async def async_close_stdin(self, p_uid: int) -> None:
        """
        Async event to close stdin.
        """
        process = self.processes[p_uid]
        stream = process.stream[self.TaskType.STDIN]
        if not stream.is_closing():
            stream.close()
            await stream.wait_closed()

        process.stdin_state = Process.StdInState.CLOSED

    def new_spawn_app(self, p: Process) -> None:
        """
        Create a task to start cmd with arguments args and identifier p_uid.
        """
        # check we do not already have a process
        # with this identifier

        # TODO should not report this as a ValueError probably
        if p.p_uid in self.processes:
            raise ValueError(f"existing managed process with p_uid={p.p_uid}")

        # add the process to our registry of processes
        self.processes[p.p_uid] = p

        # add the new process to our registry of new processes to be created.
        self.new_processes.append(p)

    def run_tasks(self, systemTasks) -> (int, int):
        """
        Run the task loop until one task returns complete. Returns a tuple
        of the set of finished system tasks and the number of remaining
        tasks to execute.
        """
        local_new_tasks = systemTasks

        # new process creation tasks are created and added
        # to the local_new_tasks list.
        local_new_tasks += self._handle_new_process_creation()

        # stdin handling
        local_new_tasks += self._handle_new_stdin_tasks()

        self.tasks.extend(local_new_tasks)

        finished = []
        unfinished = []

        # This deserves a little explanation. Asyncio in some cases
        # runs a task and completes it before the run_until_complete
        # executes below. This could happen as a result of a task
        # being created inside another task - but currently this is
        # not the case. However, it still happens under some race conditions
        # that a task completes outside of the run_until_complete below.
        # We don't want that to happen. So, we'll detect if it has happened here and flag
        # it if it happens. We'll also recover from it here by processing
        # tasks that have completed before running additional tasks.

        for task in self.tasks:
            if task.done():
                self.shepherd.log.info(f"already completed task: {task!s}")
                finished.append(task)
            elif task.cancelled():
                self.shepherd.log.info(f"cancelled task: {task!s}")
                finished.append(task)
            else:
                unfinished.append(task)

        if len(finished) == 0:
            # If the length of finished tasks is not zero then that means
            # we had a task that was already finished (see above) and we'll
            # skip running tasks right now until we have processed this
            # task that needs processing.

            if len(unfinished) == 0:
                # If there are no tasks to run and there are no finished tasks,
                # then return
                return [], 0

            finished_tasks, unfinished = asyncio.get_event_loop().run_until_complete(
                asyncio.wait(unfinished, timeout=1, return_when=asyncio.FIRST_COMPLETED)
            )

            finished.extend(finished_tasks)

        # Now accumulate the new tasks to add to the task list.
        local_new_tasks = []

        # first loop for started processes
        local_new_tasks += self._handle_started_procs(finished)

        # then handle any completed processes
        local_new_tasks += self._handle_done_procs(finished)

        # process any completed stdout and stderr reads
        local_new_tasks += self._handle_output(finished)

        self._handle_finished_stdin_tasks(finished)

        self.tasks = list(unfinished) + local_new_tasks

        # This removes the finished tasks being returned from the internal
        # reverse lookup map in the process manager.
        for task in finished:
            del self.task_tbl[task]

        return finished, len(self.tasks)

    def app_cleanup(self, p_uid: int) -> int:
        """
        Clean up all data structures associated with the process.  Returns
        the exit status of the process.
        """
        if p_uid not in self.processes:
            raise RuntimeError(f"cannot clean up non-existent process with p_uid={p_uid}")

        process = self.processes[p_uid]

        # make sure the app is not still running
        if process.state == Process.State.RUN:
            raise RuntimeError(f"cannot cleanup a running process with ID={p_uid}")

        # cancel any outstanding tasks on this app
        # There may be stdout and stderr tasks that may need clean up
        for task_id in process.stream_task:
            task = process.stream_task[task_id]
            if task is not None and not task.done():
                task.cancel()

        if process._stat is not None:
            rv = process._stat.returncode
        else:
            # None for the stat means that it never sucessfully ran.
            rv = INVALID_COMMAND

        del self.processes[p_uid]

        return rv

    def shutdown(self):
        """
        Called when the process manager is shutting down to do any clean up that is required.
        """
        proc_ids = list(self.processes.keys())
        for p_uid in proc_ids:
            self.app_cleanup(p_uid)
