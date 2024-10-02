from tornado.ioloop import PeriodicCallback
from functools import partial
import logging
import os

from odin.adapters.parameter_tree import ParameterTree
from .odin_data_util import OdinData


class MunirManager:
    """Main class for the frame processor manager object."""

    def __init__(self, ctrl_endpoints, ctrl_timeout, poll_interval, odin_data_config_path, liveivew_control, subsystem):
        """
        Initialize the controller object.

        :param ctrl_endpoints: Comma-separated list of control endpoints
        :param ctrl_timeout: Timeout value for control operations
        :param poll_interval: Poll interval for status updates
        """
        self.endpoints = [ep.strip() for ep in ctrl_endpoints.split(',')]
        self.lv = liveivew_control
        self.ctrl_timeout = ctrl_timeout

        # Create OdinData instances for each endpoint
        if len(self.endpoints) == 0:
            logging.error("Could not parse control endpoints from configuration")
        else:
            self.odin_data_instances = [OdinData(
                endpoint, odin_data_config_path, subsystem, ctrl_timeout, liveivew_control) for endpoint in self.endpoints]
        self.set_timeout(ctrl_timeout)

        # Initialize the state of control and status parameters
        self.file_path = '/tmp/'
        self.file_name = 'test'
        self.num_frames = 1000
        self.num_batches = 1
        self.fp_status = [{}] * len(self.endpoints)

        def get_arg(name):
            return getattr(self, name)

        def set_arg(name, value):
            logging.debug(f"Setting acquisition argument {name} to {value}")
            setattr(self, name, value)

        def arg_param(name):
            return (partial(get_arg, name), partial(set_arg, name))

        self.param_tree = ParameterTree({
            'endpoints': (lambda: self.endpoints, None),
            'stop_execute': (lambda: None, self.stop_acquisition),
            'start_lv_frames':(lambda: None, self.start_lv_frames),
            'timeout': (lambda: self.ctrl_timeout, self.set_timeout),
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

        logging.debug(f"Starting update task with poll interval {poll_interval} secs")
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

    def set_timeout(self, value):
        """Set the command execution timeout.

        This setting method sets the command execution timeout in seconds.

        :param value: value of the timeout set to set in seconds.
        """
        logging.debug(f"MunirManager set_timeout called with value {value}")
        self.timeout = value
        for odin_data in self.odin_data_instances:
            logging.debug(f'Setting timeout to: {value} with type: {type(value)}')
            odin_data.ctrl_timeout = value

    def _is_executing(self):
        """
        Check if the acquisition is currently executing in odin-data.

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
                logging.error(f"Failed to create acquisition for endpoint {odin_data.endpoint}")
                all_success = False

        if not all_success:
            return False

        # Start acquisition on all odin_data instances
        for odin_data in self.odin_data_instances:
            if not odin_data.start_acquisition():
                logging.error(f"Failed to start acquisition for endpoint {odin_data.endpoint}")
                all_success = False

        self._get_status()

        return all_success

    def start_lv_frames(self, *args):
        if self.lv:
            for odin_data in self.odin_data_instances:
                odin_data.start_lv()
        else:
            logging.error(f"Liveview control is disabled")

    def stop_acquisition(self, *args):
        """
        Stop the acquisition process.

        This method stops the acquisition on all connected odin-data instances.
        """
        logging.debug("Stopping acquisition")

        all_success = True

        for odin_data in self.odin_data_instances:
            if not odin_data.stop_acquisition():
                logging.error(f"Failed to stop acquisition for endpoint {odin_data.endpoint}")
                all_success = False
                
        self._get_status()
        return all_success