import logging
import zmq
import json
from datetime import datetime

class OdinData:
    """
    A class to manage connections and interactions with Odin-Data C++ applications.
    """

    def __init__(self, endpoint, config_path, subsystem, timeout):
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
        self.subsystem = subsystem
        self.config = self.load_config(config_path)

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

        :param path: Path to the JSON configuration file
        :return: Configuration dictionary for the specified subsystem
        """
        try:
            with open(f'{path}/odin_data_configs.json', 'r') as file:
                json_config = json.load(file)
                if self.subsystem in json_config:
                    logging.debug(f'Loaded {self.subsystem} config subsection: {json_config[self.subsystem]}')
                    return json_config[self.subsystem]
                else:
                    logging.error(f"No configuration found for subsystem: {self.subsystem}")
                    return {}
        except Exception as e:
            logging.error(f"Failed to load configuration: {e}")
            return {}

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
        acquisition_config = self.config.get('acquisition_config', {}).copy()

        plugin_name = None
        for key in acquisition_config.keys():
            if key != 'hdf':
                plugin_name = key
                break
        
        if plugin_name:
            acquisition_config[plugin_name]['rx_frames'] = frames
            acquisition_config['hdf']['file']['path'] = path
            acquisition_config['hdf']['frames'] = frames
            acquisition_config['hdf']['acquisition_id'] = acquisition_id
        else:
            logging.error("No valid acquisition plugin found in the configuration.")
            return False
        logging.debug(f'acquisition config: {acquisition_config}')
        return bool(self.set_config(acquisition_config))

    def start_acquisition(self):
        """
        Start the acquisition process.

        :return: True if the acquisition was started successfully, False otherwise
        """
        if not self.stop_acquisition():
            return False

        start_config = self.config.get('start_config', {})
        logging.debug(f'Start config: {start_config}')
        return bool(self.set_config(start_config))

    def stop_acquisition(self):
        """
        Stop the acquisition process.

        :return: True if the acquisition was stopped successfully, False otherwise
        """
        stop_config = self.config.get('stop_config', {})
        logging.debug(f'Stop config: {stop_config}')
        return bool(self.set_config(stop_config))

    def close(self):
        """
        Close the ZMQ connection.
        """
        self.socket.close()
        self.context.term()

    def close(self):
        """
        Close the ZMQ connection.
        """
        self.socket.close()
        self.context.term()
