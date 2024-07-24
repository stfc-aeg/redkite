import logging
import os

from functools import partial
from tornado.ioloop import PeriodicCallback
from odin.adapters.parameter_tree import ParameterTree
from .util import MunirError
from .odin_data import OdinData


class MunirFpController:
    """Main class for the frame processor controller object."""

    def __init__(self, ctrl_endpoints, ctrl_timeout, poll_interval):
        """
        Initialize the controller object.

        :param ctrl_endpoints: Comma-separated list of control endpoints
        :param ctrl_timeout: Timeout value for control operations
        :param poll_interval: Poll interval for status updates
        """
        self.endpoints = [ep.strip() for ep in ctrl_endpoints.split(',')]

        # Create OdinData instances for each endpoint
        if len(self.endpoints) == 0:
            logging.error("Could not parse control endpoints from configuration")
        else:
            self.odin_data_instances = [OdinData(endpoint) for endpoint in self.endpoints]
        self.set_timeout(1.0)

        self.ctrl_timeout = ctrl_timeout
        self._msg_id = 0

        # Initialize the state of control and status parameters
        self.do_execute = False
        self.file_path = '/tmp'
        self.file_name = 'test'
        self.num_frames = 1000
        self.num_batches = 1
        self.fp_status = [{}] * len(self.endpoints)

        def get_arg(name):
            return getattr(self, name)

        def set_arg(name, value):
            logging.debug("Setting acquisition argument %s to %s", name, value)
            setattr(self, name, value)

        def arg_param(name):
            return (partial(get_arg, name), partial(set_arg, name))

        self.param_tree = ParameterTree({
            'endpoints': (lambda: self.endpoints, None),
            'execute': (lambda: self.do_execute, self.set_execute),
            'stop_execute': (lambda: None, self.stop_acquisition),
            'timeout': (lambda: self.timeout, self.set_timeout),
            'args': {
                arg: arg_param(arg) for arg in [
                    'file_path', 'file_name', 'num_frames', 'num_batches'
                ]
            },
            'status': {
                'executing': (self._is_executing, None),
                'frames_written': (self._frames_written, None),
            },
            'frame_procs': {
                'status': (lambda: self.fp_status, None),
            }
        })

        logging.debug("Starting update task with poll interval %f secs", poll_interval)
        self.update_task = PeriodicCallback(
            self._get_status, int(poll_interval * 1000)
        )
        self.update_task.start()

    def initialize(self):
        """Initialize the controller instance.

        This method initializes the controller instance if necessary.
        """
        pass

    def cleanup(self):
        """Clean up the controller instance.

        This method cleans up the controller instances as necessary, allowing the adapter state to
        be cleaned up correctly.
        """
        self.update_task.stop()
        for odin_data in self.odin_data_instances:
            odin_data.close()

    def get(self, path):
        """Get values from the parameter tree.

        This method returns values from the parameter tree to the adapter.

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
            self.execute_acquisition()

        # Return updated values from the tree
        return self.param_tree.get(path)

    def set_timeout(self, value):
        """Set the command execution timeout.

        This setting method sets the command execution timeout in seconds.

        :param value: value of the timeout set to set in seconds.
        """
        logging.debug("MunirFpController set_timeout called with value %f", value)
        self.timeout = value
        for odin_data in self.odin_data_instances:
            logging.debug(f'Setting timout to:{value} with type:{type(value)}')
            odin_data.ctrl_timeout = value

    def set_execute(self, value):
        """Set the command execution flag.

        This setter method operates as an edge trigger, setting the internal do_execute flag if
        a command is not already running. This flag is then used to trigger execution by the
        set method once all parameters have been updated. This mechanism allows a single PUT request
        to set any other parameters and trigger an execution.

        :param value: execution flag value to set (True triggers execution)
        """
        logging.debug("MunirController set_execute called with value %s", value)

        if value:
            if not self._is_executing():
                logging.debug("Trigger acquisition execution")
                self.do_execute = True
            else:
                raise MunirError("Cannot trigger execution while acquisition is already running")

    def _is_executing(self):
        """
        Check if the acquisition is currently executing.

        :return: True if executing, False otherwise
        """
        is_executing = False
        for fp_status in self.fp_status:
            if 'hdf' in fp_status:
                is_executing |= fp_status['hdf'].get('writing', False)

        return is_executing

    def _frames_written(self):
        """
        Get the number of frames written so far.

        :return: Number of frames written
        """
        frames_written = 0
        for fp_status in self.fp_status:
            if 'hdf' in fp_status:
                frames_written += fp_status['hdf'].get('frames_written', 0)

        return frames_written

    def _next_msg_id(self):
        """
        Return the next IPC message ID to use.

        :return: Next message ID
        """
        self._msg_id += 1
        return self._msg_id

    def _get_status(self):
        """Get and display the current status of all connected odin-data instances.

        This method sends a status request to each odin-data instance and updates the
        internal fp_status list with the responses.
        """
        for idx, odin_data in enumerate(self.odin_data_instances):
            self.fp_status[idx] = odin_data.get_status()

    def execute_acquisition(self):
        """
        Execute the acquisition process.

        This method sets up the configuration for acquisition and triggers the acquisition
        on all connected odin-data instances.
        """
        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)

        logging.debug("Executing acquisition")

        all_success = True

        # Create acquisition on all odin_data instances
        for odin_data in self.odin_data_instances:
            if not odin_data.create_acquisition(self.file_path, self.file_name, self.num_frames):
                logging.error("Failed to create acquisition for endpoint %s", odin_data.endpoint)
                all_success = False

        if not all_success:
            return False

        # Start acquisition on all odin_data instances
        for odin_data in self.odin_data_instances:
            if not odin_data.start_acquisition():
                logging.error("Failed to start acquisition for endpoint %s", odin_data.endpoint)
                all_success = False

        return all_success

    def stop_acquisition(self, *args):
        """
        Stop the acquisition process.

        This method stops the acquisition on all connected odin-data instances.
        """
        logging.debug("Stopping acquisition")

        all_success = True

        for odin_data in self.odin_data_instances:
            if not odin_data.stop_acquisition():
                logging.error("Failed to stop acquisition for endpoint %s", odin_data.endpoint)
                all_success = False

        return all_success
