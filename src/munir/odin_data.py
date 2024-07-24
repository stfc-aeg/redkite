import zmq
import json
from datetime import datetime
from typing import Dict, Any, Optional

class OdinData:
    """
    A class to manage connections and interactions with Odin-Data C++ applications.
    """

    def __init__(self, endpoint: str):
        """
        Initialize the OdinData connection.

        :param endpoint: ZMQ endpoint to connect to
        """
        self.endpoint = endpoint
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.DEALER)
        self.socket.connect(endpoint)
        self.zmq_id = self.socket.getsockopt(zmq.IDENTITY)
        self.status: Dict[str, Any] = {}
        self.config: Dict[str, Any] = {}
        self.msg_id = 0
        self.ctrl_timeout = 0.0

    def _send_receive(self, msg_type: str, msg_val: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
        
        if self.socket.poll(5000):  # Wait for 5 seconds
            return self.socket.recv_json()
        else:
            raise TimeoutError(f"No response from {self.endpoint} within timeout.")

    def set_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Set the configuration for the Odin-Data application.

        :param config: Configuration dictionary
        :return: Response from the Odin-Data application
        """
        response = self._send_receive('cmd', 'configure', config)
        if response.get('msg_type') == 'ack':
            self.config.update(config)
        return response

    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the Odin-Data application.

        :return: Current status
        """
        response = self._send_receive('cmd', 'status')
        if 'params' in response:
            self.status = response['params']
        return self.status

    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration of the Odin-Data application.

        :return: Current configuration
        """
        response = self._send_receive('cmd', 'request_configuration')
        if 'params' in response:
            self.config = response['params']
        return self.config

    def create_acquisition(self, path: str, acquisition_id: str, frames: int) -> bool:
        """
        Create an acquisition setup.

        :param path: File path for the acquisition
        :param acquisition_id: ID for the acquisition
        :param frames: Number of frames for the acquisition
        :return: True if the acquisition was set up successfully, False otherwise
        """
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

    def start_acquisition(self) -> bool:
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

    def stop_acquisition(self) -> bool:
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
