"""Redkite adapter.

Redkite HiBIRDS/DPDK packet capture integration adapter.

Author: Tim Nicholls, STFC Detector Systems Software Group
"""
import logging

from odin.adapters.adapter import ApiAdapterResponse, request_types, response_types
from odin.adapters.adapter import ApiAdapter
from odin.adapters.parameter_tree import ParameterTreeError
from odin.util import decode_request_body

from .controller import RedkiteController
from .util import RedkiteError


class RedkiteAdapter(ApiAdapter):
    """Redkit adapter for HiBIRDS/DPDK control integration."""

    def __init__(self, **kwargs):
        """Initialise the adapter object.

        :param kwargs: keyawrd argument list that is passed to superclass
                       init method to populate options dictionary
        """
        # Initalise super class
        super().__init__(**kwargs)

        # Parse options from configuration
        cmd_path = str(self.options.get('cmd_path', 'unknown'))

        # Create the controller instance
        self.controller = RedkiteController(cmd_path)

        logging.debug("RedkiteAdapter loaded")

    def initialize(self, adapters):
        """Initlialise the adapter.

        This method stops the background tasks, allowing the adapter state to be cleaned up
        correctly.
        """
        logging.debug("RedkiteAdapter initalize called with %d adapters", len(adapters))
        self.controller.initialize()

    @response_types('application/json', default='application/json')
    def get(self, path, request):
        """Handle an HTTP GET request.

        This method handles an HTTP GET request, returning a JSON response.

        :param path: URI path of request
        :param request: HTTP request object
        :return: an ApiAdapterResponse object containing the appropriate response
        """
        try:
            response = self.controller.get(path)
            status_code = 200
        except ParameterTreeError as e:
            response = {'error': str(e)}
            status_code = 400

        content_type = 'application/json'

        return ApiAdapterResponse(response, content_type=content_type,
                                  status_code=status_code)

    @request_types('application/json', 'application/vnd.odin-native')
    @response_types('application/json', default='application/json')
    def put(self, path, request):
        """Handle an HTTP PUT request.

        This method handles an HTTP PUT request, decoding the request and attempting to set values
        in the asynchronous parameter tree as appropriate.

        :param path: URI path of request
        :param request: HTTP request object
        :return: an ApiAdapterResponse object containing the appropriate response
        """
        content_type = 'application/json'

        try:
            data = decode_request_body(request)
            response = self.controller.set(path, data)
            status_code = 200
        except (ParameterTreeError, RedkiteError) as e:
            response = {'error': str(e)}
            status_code = 400

        return ApiAdapterResponse(
            response, content_type=content_type, status_code=status_code
        )

    def cleanup(self):
        """Clean up the adapter.

        This method allows the adapter and controller state to be cleaned up correctly.
        """
        logging.debug("RedkitAdapter cleanup called")
        self.controller.cleanup()
