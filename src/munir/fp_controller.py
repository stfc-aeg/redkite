import logging
from functools import partial

from .util import MunirError
from .munir_manager import MunirManager
from odin.adapters.parameter_tree import ParameterTree
from odin.adapters.parameter_tree import ParameterTreeError


class MunirFpController:
    """Class to handle the instantiation of MunirManagers to control the frame-processor/odin-data 
       for different subsystems and their endpoints, and provide a central location for their 
       parameter trees to be accessed from."""
    
    def __init__(self, options: dict):
        self.munir_managers = {}
        ctrl_timeout = float(options.get('ctrl_timeout', 1.0))
        poll_interval = float(options.get('poll_interval', 1.0))
        odin_data_config_path = options.get('odin_data_config_path')
        
        subsystems = [sub.strip() for sub in (options.get('subsystems')).split(',')]
        logging.debug(f'Subsystems detected: {subsystems}')
        
        for subsystem in subsystems:
            endpoints = options.get((f'{subsystem}_endpoints'), '')
            logging.debug(f"Endpoints for {subsystem}: {endpoints}")

            # Instantiate the manager for the subsystem
            self.munir_managers[subsystem] = MunirManager(
                endpoints, ctrl_timeout, poll_interval, odin_data_config_path, subsystem)
        
        # Initialize execute flags
        self.execute_flags = {name: False for name in subsystems}

        # Setup parameter tree
        self.param_tree = ParameterTree({
            'subsystem_list': (lambda: [name for name in subsystems], None),
            'subsystems': {name: manager.param_tree for name, manager in self.munir_managers.items()},
            'execute': {name: (lambda name=name: self.execute_flags[name], partial(self.set_execute, name)) for name in subsystems}
        })

    def get(self, path):
        """Get the parameter tree."""
        return self.param_tree.get(path)

    def set(self, path, data):
        """Set parameters in the parameter tree."""
        logging.debug("Calling MunirFpController Set method")
        try:
            # Set the parameters in the parameter tree
            self.param_tree.set(path, data)
            subsystem = self.parse_subsystem(path, data)

            if path == 'execute/' and data.get(subsystem, False):
                logging.debug("Calling _handle_execution ")
                self._handle_execution(subsystem)

        except ParameterTreeError as e:
            # Raise a custom error if a parameter tree error occurs
            raise MunirFpControllerError(e)
        
    def parse_subsystem(self, path, data):
        """ Extract the subsystem name from the request sent to the SET method"""
        subsystem = None
        if path == 'execute/':
            # If the path is 'execute/', the key of the data will be the subsystem name
            subsystem = list(data.keys())[0]
            logging.debug(f"Subsystem derived from execute path: {subsystem}")
        elif path.startswith('subsystems/'):
            # If the path starts with 'subsystems', use the second part of the path
            subsystem = path.split('/')[1]
            logging.debug(f"Subsystem derived from path: {subsystem}")
        else:
            # Handle any other cases
            subsystem = None
            logging.debug(f"Subsystem not determined from path: {path}")
        return subsystem

    def set_execute(self, subsystem_name, value):
        """Set the command execution flag for a subsystem.

        :param subsystem_name: Name of the subsystem
        :param value: execution flag value to set (True triggers execution)
        """
        if value:
            if not self.munir_managers[subsystem_name]._is_executing():
                logging.debug(f"Trigger execution set for {subsystem_name}")
                self.execute_flags[subsystem_name] = True
            else:
                raise MunirError(f"Cannot trigger execution for {subsystem_name} while acquisition is already running")
        else:
            self.execute_flags[subsystem_name] = False

    def _handle_execution(self, subsystem_name):
        """Handle execution of acquisiton on a subsystem.

        :param subsystem_name: Name of the subsystem 
        """
        if self.execute_flags.get(subsystem_name, False):
            manager = self.munir_managers[subsystem_name]
            # Ensure the manager is not already executing
            if not manager._is_executing():
                logging.debug(f"Calling execute_acquisition for subsystem {subsystem_name}")
                # Trigger the execution process
                success = manager.execute_acquisition()
                if success:
                    # Reset the execute flag after successful execution
                    self.execute_flags[subsystem_name] = False
            else:
                # Debug log if the subsystem is already executing
                logging.debug(f"Cannot trigger execution for {subsystem_name} while acquisition is already running")       

class MunirFpControllerError(Exception):
    pass