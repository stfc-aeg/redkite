import logging
import zmq
import json
from datetime import datetime

class OdinData:
    """
    A class to manage connections and interactions with Odin-Data C++ applications.
    """

    def __init__(self, endpoint, config_path, timeout):
        """
        Initialize the OdinData connection.

        :param endpoint: ZMQ endpoint to connect to
        """
        self.endpoint = endpoint
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(endpoint)
        self.zmq_id = self.socket.getsockopt(zmq.IDENTITY)
        self.status = {}
        self.config = {}
        self.msg_id = 0
        self.ctrl_timeout = timeout

        self.load_config(config_path)

    def _send_receive(self, msg_type, msg_val, params=None):
        """
        Send a message to the Odin-Data application and receive the response.

        :param msg_type: Type of the message
        :param msg_val: Value of the message
        :param params: Additional parameters for the message
        :return: Response from the Odin-Data application
        """
        self.msg_id += 1
        message = {
            'msg_type': msg_type,
            'msg_val': msg_val,
            'timestamp': datetime.now().isoformat(),
            'id': self.msg_id,
            'params': params or {}
        }

        self.socket.send_json(message)
        if self.socket.poll(int(self.ctrl_timeout*1000)):
            return self.socket.recv_json()
        else:
            logging.error(f"No response from {self.endpoint} within timeout of: {self.ctrl_timeout}s .")
            return {}
        
    def load_config(self, path):
        """
        Load configuration from a JSON file.

        :param config_file: Path to the JSON configuration file
        """
        try:
            with open((f'{path}/odin_data_configs.json'), 'r') as file:
                self.json_config = json.load(file)
                logging.debug(self.json_config)
        except Exception as e:
            logging.error(f"Failed to load configuration file: {e}")
            self.json_config = {}

    def set_config(self, config):
        """
        Set the configuration for the Odin-Data application.

        :param config: Configuration dictionary
        :return: Response from the Odin-Data application
        """
        response = self._send_receive('cmd', 'configure', config)
        if response.get('msg_type') == 'ack':
            self.config.update(config)
        return response

    def get_status(self):
        """
        Get the current status of the Odin-Data application.

        :return: Current status
        """
        response = self._send_receive('cmd', 'status')
        if 'params' in response:
            self.status = response['params']
        return self.status

    def get_config(self):
        """
        Get the current configuration of the Odin-Data application.

        :return: Current configuration
        """
        response = self._send_receive('cmd', 'request_configuration')
        if 'params' in response:
            self.config = response['params']
        return self.config

    def create_acquisition(self, path, acquisition_id, frames):
        """
        Create an acquisition setup.

        :param path: File path for the acquisition
        :param acquisition_id: ID for the acquisition
        :param frames: Number of frames for the acquisition
        :return: True if the acquisition was set up successfully, False otherwise
        """

        # make setting of config generic, it needs to adapt to,different types of config
        # json.loads string to dict 
        common_config = {
            "hibirdsdpdk": {
                "update_config": True,
                "rx_enable": False,
                "proc_enable": True,
                "rx_frames": frames
            },
            "hdf": {
                "write": False
            }
        }

        if not self.set_config(common_config):
            return False

        acquisition_config = {
            "hibirdsdpdk": common_config["hibirdsdpdk"],
            "hdf": {
                "file": {
                    "path": path
                },
                "frames": frames,
                "acquisition_id": acquisition_id,
                "write": False
            }
        }

        return bool(self.set_config(acquisition_config))

    def start_acquisition(self):
        """
        Start the acquisition process.

        :return: True if the acquisition was started successfully, False otherwise
        """
        if not self.stop_acquisition():
            return False

        start_config = {
            "hibirdsdpdk": {
                "update_config": True,
                "rx_enable": True
            },
            "hdf": {
                "write": True
            }
        }

        return bool(self.set_config(start_config))

    def stop_acquisition(self):
        """
        Stop the acquisition process.

        :return: True if the acquisition was stopped successfully, False otherwise
        """
        stop_config = {
            "hibirdsdpdk": {
                "update_config": True,
                "rx_enable": False,
                "proc_enable": True
            },
            "hdf": {
                "write": False
            }
        }

        return bool(self.set_config(stop_config))

    def close(self):
        """
        Close the ZMQ connection.
        """
        self.socket.close()
        self.context.term()
