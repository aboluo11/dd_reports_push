import asyncio
import os
import pickle
import signal
import traceback

class AsyncProcessRunner:
    def __init__(self, target_func, *args, timeout=None, **kwargs):
        self.target_func = target_func
        self.args = args
        self.kwargs = kwargs
        self.timeout = timeout
        self.process = None
        self.read_fd, self.write_fd = os.pipe()

    async def run(self):
        loop = asyncio.get_running_loop()

        self.process = os.fork()
        if self.process == 0:  # Child process
            os.close(self.read_fd)
            try:
                result = self.target_func(*self.args, **self.kwargs)
                os.write(self.write_fd, pickle.dumps(result))
            except Exception as e:
                os.write(self.write_fd, pickle.dumps((False, str(e), traceback.format_exc())))
            finally:
                os._exit(0)
        else:  # Parent process
            os.close(self.write_fd)
            
            reader = asyncio.StreamReader()
            protocol = asyncio.StreamReaderProtocol(reader)
            
            transport, _ = await loop.connect_read_pipe(lambda: protocol, os.fdopen(self.read_fd))
            
            try:
                result_data = await asyncio.wait_for(reader.read(), timeout=self.timeout)
                result = pickle.loads(result_data)
                return result
            except asyncio.TimeoutError:
                print(f"Process execution timed out after {self.timeout} seconds")
                self.terminate_process()
                return False
            except Exception as e:
                print(f"Error reading result from child process: {e}")
                self.terminate_process()
                return False
            finally:
                if self.process:
                    try:
                        os.waitpid(self.process, 0)
                    except ChildProcessError:
                        pass

    def terminate_process(self):
        if self.process:
            print(f"Terminating process {self.process}")
            try:
                os.kill(self.process, signal.SIGKILL)
                _, exit_code = os.waitpid(self.process, 0)
                print(f"Process {self.process} terminated with exit code {exit_code}")
            except Exception as e:
                print(f"Error terminating process: {e}")
            finally:
                self.process = None
