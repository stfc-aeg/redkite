import logging
import zmq
from zmq.utils.monitor import parse_monitor_message
from zmq.eventloop.zmqstream import ZMQStream
from zmq.eventloop import ioloop
from zmq.constants import Event
from odin_data.control.ipc_channel import IpcChannel

class MonitoredIpcChannel(IpcChannel):
    """
    Extended IpcChannel class to include a monitor_socket that manages the connection state using callbacks.
    """
    def __init__(self, channel_type, endpoint=None, context=None, identity=None):
        # Use the tornado IOLoop
        self._ioloop = ioloop.IOLoop.current()
        self.connection_status = False
        
        context = context or zmq.Context.instance()
        super().__init__(channel_type, endpoint, context, identity)
        
        self._monitor_socket = self.socket.get_monitor_socket(
            IpcChannel.EVENT_ACCEPTED | IpcChannel.EVENT_DISCONNECTED
        )

        # Create a ZMQStream for the monitor socket using the existing IOLoop
        self._monitor_stream = ZMQStream(self._monitor_socket, io_loop=self._ioloop)

        # Set up the callback func to call on events detected
        self._monitor_stream.on_recv(self._handle_monitor_event)

    def _handle_monitor_event(self, msg):
        """
        Handle events from the monitor socket.
        """
        event_msg = parse_monitor_message(msg)
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

    def check_connection(self):
        """
        Return the current connection status.
        """
        return self.connection_status

    def close(self):
        """
        Clean up resources.
        """
        self._monitor_stream.close()
        self.socket.disable_monitor()
        super().close()
