"""Redkite controller class.

This class implements the Redkite controller, which is responsible for configuring and
executing the HiBIRDS/DPDK packet capture application as a subprocess. Various parameters
of the application can be configured via a parameter tree, and the state of the executed process
and its output are captured and exposed to the adapter.

Author: Tim Nicholls, STFC Detector Systems Software Group
"""
import logging
import subprocess
from concurrent import futures

from tornado.concurrent import run_on_executor

from odin.adapters.parameter_tree import ParameterTree

from .util import RedkiteError


class RedkiteController():
    """Main class for the controller object."""

    # Thread executor used for process execution
    executor = futures.ThreadPoolExecutor(max_workers=1)

    def __init__(self, cmd_path):
        """Initialise the controller object.

        This constructor initlialises the controller object, building a parameter tree to control
        the packet capture application parameters and report status of execution
        """
        # Set the capture command path
        self.cmd_path = cmd_path

        # Initialise the state of control and status parameters
        self.executing = False
        self.do_execute = False

        self.file_path = "/tmp"
        self.file_name = "capture.bin"
        self.num_frames = 100000
        self.num_files = 1

        self.return_code = None
        self.last_command = None
        self.stdout = None
        self.stderr = None
        self.exception = None

        # Build the parameter tree
        self.param_tree = ParameterTree({
            'cmd_path': self.cmd_path,
            'file_path': (lambda: self.file_path, self.set_file_path),
            'file_name': (lambda: self.file_name, self.set_file_name),
            'num_frames': (lambda: self.num_frames, self.set_num_frames),
            'num_files': (lambda: self.num_files, self.set_num_files),
            'execute': (lambda: self.executing, self.set_execute),
            'status': {
                'executing': (lambda: self.executing, None),
                'return_code': (lambda: self.return_code, None),
                'last_command': (lambda: self.last_command, None),
                'stdout': (lambda: self.stdout, None),
                'stderr': (lambda: self.stderr, None),
                'exception': (lambda: self.exception, None),
            }
        })

    def initialize(self):
        """Initialize the controller instance.

        This method intialises the controller instance if necessary.
        """
        pass

    def cleanup(self):
        """Clean up the controller instance.

        This method stops the background tasks, allowing the adapter state to be cleaned up
        correctly.
        """
        pass

    def get(self, path):
        """Get values from the parameter tree.

        This method returns values from parameter tree to the adapter.

        :param path: path to retrieve from tree
        """
        return self.param_tree.get(path)

    def set(self, path, data):
        """Set values in the parameter tree.

        This method sets values in the parameter tree. If a command execution was requested in the
        data, trigger the execution.

        :param path: path of data to set in the tree
        :param data: data to set in tree
        """
        # Update values in the tree at the specified path
        self.param_tree.set(path, data)

        # If the call included trigger an execution do so now all parameters have been updated.
        if self.do_execute:
            self.do_execute = False
            self._execute_cmd()

        # Return updated values from the tree
        return self.param_tree.get(path)

    def set_file_path(self, file_path):
        """Set the output file path.

        This setter function sets the output file path passed to the executed application.

        :param file_path: path to write output files to
        """
        logging.debug("RedkiteAdapter set_file_path called with path %s", file_path)
        self.file_path = file_path

    def set_file_name(self, file_name):
        """Set the output file name.

        This setter function sets the name of output file passed to the excecuted application.

        :param file_name: name of the output file
        """
        logging.debug("RedkiteAdapter set_file_name called with name %s", file_name)
        self.file_name = file_name

    def set_num_frames(self, num_frames):
        """Set the number of frames.

        This setter function sets the number of frames to be written by the excecuted application.

        :param num_frames: number of frames
        """
        logging.debug("RedkiteAdapter set_num_frames called with name %d", num_frames)
        self.num_frames = num_frames

    def set_num_files(self, num_files):
        """Set the number of files.

        This setter function sets the number of output file passed to the excecuted application.

        :param num_files: number of files
        """
        logging.debug("RedkiteAdapter set_num_files called with name %d", num_files)
        self.num_files = num_files

    def set_execute(self, value):
        """Set the command execution flag.

        This setter method operates as an edge trigger, setting the internal do_execute flag if
        a command is not already running. This flag is then used to trigger execution by the
        set method once all parameters have been updated. This mechanism allows a single PUT request
        to set any other parameters and trigger an execution.

        :param value: execution flag value to set (True triggers excecution)
        """
        logging.debug("RedkiteAdapter set_execute called with value %s", value)

        if value:
            if not self.executing:
                logging.debug("Trigger command execution")
                self.do_execute = True
            else:
                raise RedkiteError("Cannot trigger execution while command is already running")

    @run_on_executor
    def _execute_cmd(self):
        """Execute the external command.

        This internal method executes the specified external command as a subprocess. The args
        passed to the command are assembled from parameters, the subprocess is executed and the
        result captured in status parameters. This method runs on a thread pool executor to not
        block the main event loop.
        """
        # Set executing flag
        self.executing = True

        # Assemble command arguments
        cmd_args = [
            self.cmd_path, "udp_rx",
            "--filename", self.file_name,
            "--path", self.file_path,
            "--frames", str(self.num_frames),
            "--batches", str(self.num_files),
        ]

        # Retain the full command for debugging
        self.last_command = ' '.join([str(arg) for arg in cmd_args])

        logging.debug("Executing command %s", self.last_command)

        # Execute the command in a subprocess, capturing the result in status parameters. Exceptions
        # are also trapped and recorded in parameters.
        try:
            result = subprocess.run(cmd_args, capture_output=True)
        except (TypeError, subprocess.SubprocessError) as error:
            logging.error("Execution of command failed: %s", error)
            self.exception = str(error)
        else:
            self.exception = None
            self.return_code = result.returncode
            self.stdout = result.stdout.decode("utf-8")
            self.stderr = result.stderr.decode("utf-8")
            if self.return_code == 0:
                logging.debug("Execution of command completed OK")
            else:
                logging.error("Execution of command failed return code %d", self.return_code)

        # Clear the executing flag
        self.executing = False
