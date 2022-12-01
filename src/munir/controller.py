"""Munir controller class.

This class implements the Munir controller, which is responsible for configuring and
executing the HiBIRDS/DPDK packet capture application as a subprocess. Various parameters
of the application can be configured via a parameter tree, and the state of the executed process
and its output are captured and exposed to the adapter.

Author: Tim Nicholls, STFC Detector Systems Software Group
"""
import logging
import re
import subprocess
from ast import literal_eval
from concurrent import futures
from functools import partial

from tornado.concurrent import run_on_executor

from odin.adapters.parameter_tree import ParameterTree

from .util import MunirError


class MunirController():
    """Main class for the controller object."""

    # Thread executor used for process execution
    executor = futures.ThreadPoolExecutor(max_workers=1)

    def __init__(self, cmd_template, timeout):
        """Initialise the controller object.

        This constructor initlialises the controller object, building a parameter tree to control
        the target command parameters and report status of execution. The target command is
        specified in template form, with modifiable arguments and their default values expressed
        in curly braces, e.g. {file_name:test.txt}

        :param cmd_template: template of command to execute
        :param timeout: command execution timeout in seconds
        """
        self.args = self.parse_cmd_template(cmd_template)
        self.timeout = timeout

        # Initialise the state of control and status parameters
        self.executing = False
        self.do_execute = False

        self.return_code = None
        self.last_command = None
        self.stdout = None
        self.stderr = None
        self.exception = None

        # Build the parameter tree
        self.param_tree = ParameterTree({
            'cmd_template': self.cmd_template,
            'execute': (lambda: self.executing, self.set_execute),
            'timeout': (lambda: self.timeout, self.set_timeout),
            'args': self.args,
            'status': {
                'executing': (lambda: self.executing, None),
                'return_code': (lambda: self.return_code, None),
                'last_command': (lambda: self.last_command, None),
                'stdout': (lambda: self.stdout, None),
                'stderr': (lambda: self.stderr, None),
                'exception': (lambda: self.exception, None),
            }
        })

    def parse_cmd_template(self, cmd_template):
        """Parse the command template and initialise modifiable arguments.

        This method parses the specified command template, identifying modifable arguments specified
        by template substitutions (marked with curly braces) and building a dictionary of those
        arguments initialised with default values if given and structured as parameter accessor
        getter/setter pairs for use in a parameter tree.

        :param cmd_template: command template as a string
        :return args: dictionary of modifiable arguments with parameter accessor pairs
        """
        # Initialise internal state of arguments and full command argument list
        self._args = {}
        self.cmd_args = []
        args = {}

        def get_arg(name):
            """Get the current value of a command argument.

            This inner function returns the current value of the named command argument, and is
            used to dynamically generate getter partials for modifiable parameters

            :param name: name of argument
            :return current value of argument
            """
            return self._args[name]

        def set_arg(name, value):
            """Set the current value of a command argument.

            This inner function returns the current value of the named command argument, and is
            used to dynamically generate setter partials for modifiable parameters

            :param name: name of argument
            :param value: value to set argument to
            """
            logging.debug("Setting command argument %s to %s", name, value)

            self._args[name] = value

        # Strip any line breaks from the command template and store
        self.cmd_template = cmd_template.replace('\n', ' ')
        if not self.cmd_template:
            logging.error("No command template specified")
            return args

        # Iterate through arguments in command template and build a dict of settable arguments to
        # return. Also construct a list of getters for the full command argument list to allow
        # the command to be built at execution time.
        for arg in self.cmd_template.split(' '):

            # If the current argument is a template subsitution marked with curly braces, parse to
            # into the settable argument list. If a default value is specified, infer the argument
            # type and set the initial value accordingly. If no default is given, assume the
            # argument is a string and initalise empty.
            param = re.match(r"^{(\S+)}$", arg)
            if param:
                # Extract the argument template elements
                elems = param.group(1).split(':')
                name = elems[0]
                # If a default value is given, evaluate it
                if len(elems) >= 2:
                    try:
                        value = literal_eval(elems[1])
                    except Exception:
                        value = elems[1]
                else:
                    value = ""

                # Set the initial value of the argument
                self._args[name] = value

                # Bind getter and setter partials into the argument dict
                getter = partial(get_arg, name)
                setter = partial(set_arg, name)
                args[name] = (getter, setter)

                # Append the argument getter to the full command argument list
                self.cmd_args.append(getter)
            else:
                # If this argument is not a template subsitution, append a getter for the value of
                # the argument to the full command argument list
                self.cmd_args.append(lambda arg=arg: arg)

        # Log a warning if parsing the command template did not yield an executable command
        if not self.cmd_args:
            logging.warning(
                "Parsing the specified command template did not yield an executable command"
            )

        # Return the modifiable argument dict for use in a parameter tree
        return args

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

    def set_execute(self, value):
        """Set the command execution flag.

        This setter method operates as an edge trigger, setting the internal do_execute flag if
        a command is not already running. This flag is then used to trigger execution by the
        set method once all parameters have been updated. This mechanism allows a single PUT request
        to set any other parameters and trigger an execution.

        :param value: execution flag value to set (True triggers excecution)
        """
        logging.debug("MunirController set_execute called with value %s", value)

        if value:
            if not self.executing:
                logging.debug("Trigger command execution")
                self.do_execute = True
            else:
                raise MunirError("Cannot trigger execution while command is already running")

    def set_timeout(self, value):
        """Set the command execution timeout.

        This setting method sets the command execution timeout in seconds.

        :param value: value of the timeout set to set in seconds.
        """
        logging.debug("MunirController set_timeout called with value %d", value)
        self.timeout = value

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

        # Assemble command argument list by evaluating current value of each argument
        cmd_args = [str(arg()) for arg in self.cmd_args]

        # Retain the full command for debugging
        self.last_command = ' '.join([str(arg) for arg in cmd_args])

        logging.debug("Executing command %s", self.last_command)

        # Execute the command in a subprocess, capturing the result in status parameters. Exceptions
        # are also trapped and recorded in parameters.
        try:
            result = subprocess.run(cmd_args, capture_output=True, timeout=self.timeout)
        except subprocess.TimeoutExpired:
            error = "Execution of command timed out after {:d} seconds".format(self.timeout)
            logging.error(error)
            self.exception = error
            self.return_code = -1
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
