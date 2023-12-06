import logging
import os

from functools import partial

from tornado.ioloop import PeriodicCallback

from odin.adapters.parameter_tree import ParameterTree

from odin_data.control.ipc_channel import IpcChannel
from odin_data.control.ipc_message import IpcMessage

from .util import MunirError


class MunirFpController():
    """Main class for the frame processor controller object."""

    def __init__(self, ctrl_endpoints, ctrl_timeout, poll_interval):

        self.endpoints = [ep.strip() for ep in ctrl_endpoints.split(',')]
        self.set_timeout(1.0)

        self.ctrl_timeout = ctrl_timeout

        if len(self.endpoints) == 0:
            logging.error("Could not parse contrl endpoints from configuration")

        self.ctrl_channels = []

        for endpoint in self.endpoints:

            channel = IpcChannel(IpcChannel.CHANNEL_TYPE_DEALER)
            channel.connect(endpoint)
            self.ctrl_channels.append(channel)

            logging.debug(
                "Control channel created with endpoint %s identity %s",
                endpoint, channel.identity
            )

        self._msg_id = 0

        # Initialise the state of control and status parameters
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
            'timeout': (lambda: self.timeout, self.set_timeout),
            'args': {
                arg : arg_param(arg) for arg in [
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

        This method intialises the controller instance if necessary.
        """
        pass

    def cleanup(self):
        """Clean up the controller instance.

        This method cleans up the controller instances as necessary, allowing the adapter state to
        be cleaned up correctly.
        """
        self.update_task.stop()
        for ctrl_channel in self.ctrl_channels:
            ctrl_channel.close()

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
            if not self._is_executing():
                logging.debug("Trigger acquisition execution")
                self.do_execute = True
            else:
                raise MunirError("Cannot trigger execution while acquisition is already running")

    def _is_executing(self):

        is_executing = False
        for fp_status in self.fp_status:
            if 'hdf' in fp_status:
                is_executing |= fp_status['hdf'].get('writing', False)

        return is_executing

    def _frames_written(self):

        frames_written = 0
        for fp_status in self.fp_status:
            if 'hdf' in fp_status:
                frames_written += fp_status['hdf'].get('frames_written', 0)

        return frames_written

    def _next_msg_id(self):
        """Return the next IPC message ID to use."""
        self._msg_id += 1
        return self._msg_id

    def await_response(self, channel):
        """Await a response to a client command on the given channel."""
        pollevents = channel.poll(int(self.ctrl_timeout * 1000))
        if pollevents == IpcChannel.POLLIN:
            reply = IpcMessage(from_str=channel.recv())
            # logging.debug(f"Got response from {channel.identity}: {reply}")
            return reply
        else:
            logging.error(f"No response received or error occurred from {channel.identity}.")
            return False

    def _get_status(self):
        """Get and display the current status of all connected odin-data instances."""
        #status_responses = []
        status_msg = IpcMessage('cmd', 'status', id=self._next_msg_id())

        for ctrl_channel in self.ctrl_channels:
            #logging.debug(f"Sending status request to {ctrl_channel.identity}")
            ctrl_channel.send(status_msg.encode())

        for (idx, ctrl_channel) in enumerate(self.ctrl_channels):
            response = self.await_response(ctrl_channel)
            if response:
                self.fp_status[idx] = response.get_params()

    def send_config_message(self, config):
        """Send a configuration message to all instances."""
        all_responses_valid = True

        for channel in self.ctrl_channels:
            config_msg = IpcMessage('cmd', 'configure', id=self._next_msg_id())
            config_msg.set_params(config)
            logging.debug(f"Sending configuration: {config} to {channel.identity}")
            channel.send(config_msg.encode())
            if not self.await_response(channel):  # pass the channel to await_response
                all_responses_valid = False

        return all_responses_valid

    def execute_acquisition(self):

        if not os.path.exists(self.file_path):
            os.makedirs(self.file_path)

        logging.debug("Executing acquisition")

        # Send initial config message to disable packet RX/processing and turn off file writing
        config = {
            "hibirdsdpdk": {
                "update_config": True,
                "rx_enable": False,
                "proc_enable": True,             # TODO - why is this not false?
                "rx_frames": self.num_frames
            },
            "hdf": {
                "write": False
            }
        }

        logging.debug("Sending initial acquisition config message with params %s", config)
        if not self.send_config_message(config):
            logging.error("Failed to send initial config, aborting acquisition")
            return False

        # Set up the HDF file writing plugin with the appropriate parameters
        hdf_config = {
            "hdf": {
                "file": {
                    "path": self.file_path,
                    "name": self.file_name,
                },
                "frames": self.num_frames,
                "write": True
            }
        }

        logging.debug("Sending HDF config message with params %s", config)
        if not self.send_config_message(hdf_config):
            logging.error("Failed to send HDF config, aborting acquisition")
            return False

        # Arm the packet processing cores
        proc_config = {
            "hibirdsdpdk": {
                "update_config": True,
                "rx_enable": False,
                "proc_enable": True,
            }
        }

        logging.debug("Sending packet processing config message with params %s", config)
        if not self.send_config_message(proc_config):
            logging.error("Failed to send packet processing config, aborting acquisition")
            return False

        # Enable packet reception
        rx_config = {
            "hibirdsdpdk": {
                "update_config": True,
                "rx_enable": True,
            }
        }

        logging.debug("Sending packet RX config message with params %s", config)
        if not self.send_config_message(rx_config):
            logging.error("Failed to send packet RX config, aborting acquisition")
            return False

        return True
