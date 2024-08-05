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
                endpoints, ctrl_timeout, poll_interval, odin_data_config_path)
        
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
        try:
            # Set the parameters in the parameter tree
            self.param_tree.set(path, data)

            # Check execute flags after setting parameters
            subsystem_name = path.split('/')[1] 
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

        except ParameterTreeError as e:
            # Raise a custom error if a parameter tree error occurs
            raise MunirFpControllerError(e)

    def set_execute(self, subsystem_name, value):
        """Set the command execution flag for a subsystem.

        :param subsystem_name: Name of the subsystem
        :param value: execution flag value to set (True triggers execution)
        """
        if value:
            if not self.munir_managers[subsystem_name]._is_executing():
                logging.debug("Trigger execution set for %s", subsystem_name)
                self.execute_flags[subsystem_name] = True
            else:
                raise MunirError("Cannot trigger execution for %s while acquisition is already running" % subsystem_name)
        else:
            self.execute_flags[subsystem_name] = False
        
class MunirFpControllerError(Exception):
    pass