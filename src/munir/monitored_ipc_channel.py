import logging
from zmq.utils.monitor import parse_monitor_message
from zmq.constants import Event
from odin_data.control.ipc_channel import IpcChannel

class MonitoredIpcChannel(IpcChannel):
    """
    Extended IpcChannel class to include a monitor_socket that will check for connection status
    events and return the status of the connection.
    """
    def __init__(self, channel_type, endpoint=None, context=None, identity=None):
        super().__init__(channel_type, endpoint, context, identity)

        self._monitor_socket = self.socket.get_monitor_socket(
            IpcChannel.EVENT_ACCEPTED | IpcChannel.EVENT_DISCONNECTED
        )
        self.connection_status = False

    def check_connection(self):
        """
        Check and update the connection status by polling the monitor socket for connection events.
        """
        try:
            if self._monitor_socket.poll(0) == IpcChannel.POLLIN:
                event_msg = parse_monitor_message(self._monitor_socket.recv_multipart())
                event = event_msg["event"]
                logging.debug(f"Event ID received: {event} | {Event(event).name}")
                if event == IpcChannel.EVENT_ACCEPTED:
                    if not self.connection_status:
                        logging.info("Socket connection established.")
                    self.connection_status = True
                elif event == IpcChannel.EVENT_DISCONNECTED:
                    if self.connection_status:
                        logging.warning("Socket connection lost.")
                    self.connection_status = False
        except Exception as e:
            logging.error(f"Monitor socket Exception: {e}")
            self.connection_status = False
        except:
            self.connection_status = False
        return self.connection_status