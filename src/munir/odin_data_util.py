import logging
import zmq
import json
from datetime import datetime, time
from time import sleep

from .monitored_ipc_channel import MonitoredIpcChannel

from odin_data.control.ipc_channel import IpcChannel
from odin_data.control.ipc_message import IpcMessage

class OdinData:
    """
    A class to manage a connection and interactions with an Odin-Data C++ application instance.
    """

    def __init__(self, endpoint, config_path, subsystem, timeout, liveivew_control):
        """
        Initialise the OdinData connection.

        :param endpoint: IpcChannel endpoint to connect to
        """
        self.endpoint = endpoint
        self.channel = MonitoredIpcChannel(IpcChannel.CHANNEL_TYPE_DEALER, endpoint)
        self.channel.connect(endpoint)
        self.status = {}
        self.config = {}
        self.msg_id = 0
        self.ctrl_timeout = timeout
        self.subsystem = subsystem
        self.config = self.load_config(config_path)
        self.lv = liveivew_control
        logging.debug(f"Liveview control for {self.subsystem} {'enabled' if self.lv else 'disabled'}")
        if self.lv:
            self.start_lv()

    def _send_receive(self, msg_type, msg_val, params=None):
        """
        Send a message to the Odin-Data application and receive and filter any responses.

        :param msg_type: Type of the message
        :param msg_val: Value of the message
        :param params: Additional parameters for the message
        :return: Response from the Odin-Data application
        """
        #Use the MonitoredIpcChannel function to use the monitor_socket to check for socket connection status
        if not self.channel.check_connection():
            # Debug no conneciton, and return an empty response
            logging.error(f"Cannot send message to {self.endpoint}, socket is disconnected.")
            return {}
        
        # Build IpcMessage and send using IpcChannel send funciton
        self.msg_id += 1
        message = IpcMessage(msg_type=msg_type, msg_val=msg_val, id=self.msg_id)
        if params:
            message.set_params(params)
        self.channel.send(message.encode())

        # Recevie all data on the socket for up to a cfg defined timeout or until no data:
        while self.channel.poll(int(self.ctrl_timeout * 1000)) == IpcChannel.POLLIN:
            # procoess responses until response containing last msg_id sent is found and discard the rest
            response_data = self.channel.recv()
            response = IpcMessage(from_str=response_data)
            if response.is_valid():
                if response.attrs.get('id') == self.msg_id:
                    return response.attrs
                else:
                    logging.warning(f"Received message with id of: {response.attrs.get('id')} when last sent was: {self.msg_id} | Dropping message" )
            else:
                logging.error("Invalid response received")
                return {}
        logging.error(f"No response from {self.endpoint} within timeout of: {self.ctrl_timeout}s.")
        return {}
        
        # using get_socket_monitor to poll for when the connection is re-established. 
        
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
        if response.get('msg_type') == IpcMessage.ACK:
            self.config.update(config)
        return response

    def get_status(self):
        """
        Get the current status of the Odin-Data application.

        :return: Current status
        """
        response = self._send_receive('cmd', 'status')
        if 'params' in response:
            logging.debug(f"Internal MSG ID: {self.msg_id} | Recieved MSG ID: {response['id']}")
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
        if not self.stop_acquisition():
            return False
        # allow time for config to propogate in odin_data
        sleep(0.01)
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
        # if not self.stop_acquisition():
        #     return False

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

    def start_lv(self):
        """Start lv"""
        if self.lv:
            if not self.stop_acquisition():
                return False
            # allow time for config to propogate in odin_data
            sleep(0.01)
            arm_config = self.config.get('arm_config')
            logging.debug(f'arm: {arm_config}')
            if not (self.set_config(arm_config)):
                return False
            # allow time for config to propogate in odin_data
            sleep(0.01)
            lv_config = self.config.get('lv_config')
            logging.debug(f'lv: {lv_config}')
            return bool(self.set_config(lv_config))
        else:
            logging.error(f"Liveview control is disabled")

    def close(self):
        """
        Close the ZMQ connection.
        """
        self.channel.close()